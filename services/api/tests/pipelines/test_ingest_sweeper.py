"""Automatic ingestion: the sweep logic with fake Blob, Kubernetes and Ray adapters, and the deploy manifests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from pipelines.ingestion.sweeper import BlobInfo, find_pending, run_pass, tenant_of

ROOT = Path(__file__).resolve().parents[4]
NOW = datetime(2026, 11, 16, 15, 0, tzinfo=timezone.utc)
OLD = NOW - timedelta(hours=1)


def _blob(name, etag="e1", modified=OLD):
    return BlobInfo(name, etag, modified)


class _Store:
    def __init__(self, blobs, state=None):
        self.blobs, self.state, self.saves = blobs, state or {}, 0

    def list_uploads(self):
        return self.blobs

    def load_state(self):
        return dict(self.state)

    def save_state(self, state):
        self.state, self.saves = dict(state), self.saves + 1


class _Cluster:
    def __init__(self, owns=True):
        self.owns, self.events = owns, []

    def resume(self):
        self.events.append("resume")
        return self.owns

    def wait_ready(self):
        self.events.append("ready")

    def suspend(self):
        self.events.append("suspend")


class _Ray:
    def __init__(self, outcomes=None):
        self.outcomes, self.submitted = outcomes or {}, []

    def submit(self, entrypoint, env, sid):
        self.submitted.append((entrypoint, env, sid))

    def status(self, sid):
        entrypoint = next(e for e, _, s in self.submitted if s == sid)
        return self.outcomes.get(entrypoint.split()[-1], "SUCCEEDED")


def _sweep(store, cluster, ray, **kw):
    return run_pass(store=store, cluster=cluster, ray=ray, base_env={"A": "1"}, container="documents",
                    now=lambda: NOW, sleep=lambda _: None, **kw)


def test_tenant_comes_from_the_path_and_unsafe_or_non_upload_paths_are_ignored() -> None:
    assert tenant_of("uploads/acme/a.pdf") == "acme"
    assert tenant_of("uploads/acme/sub/a.pdf") == "acme"
    for bad in ("other/acme/a.pdf", "uploads/a.pdf", "uploads/../x/a.pdf", "uploads/a b/x.pdf", "uploads/acme/", "_state/x.json"):
        assert tenant_of(bad) is None


def test_new_changed_and_in_progress_blobs() -> None:
    state = {
        "uploads/t/done.pdf": {"etag": "e1", "done": True, "attempts": 1},
        "uploads/t/changed.pdf": {"etag": "old", "done": True, "attempts": 1},
        "uploads/t/retry.pdf": {"etag": "e1", "done": False, "attempts": 1},
        "uploads/t/gave-up.pdf": {"etag": "e1", "done": False, "attempts": 3},
    }
    blobs = [_blob("uploads/t/done.pdf"), _blob("uploads/t/changed.pdf"), _blob("uploads/t/retry.pdf"),
             _blob("uploads/t/gave-up.pdf"), _blob("uploads/t/new.pdf"),
             _blob("uploads/t/uploading.pdf", modified=NOW - timedelta(seconds=10))]
    names = [b.name for b in find_pending(blobs, state, NOW, quiet_seconds=120, max_attempts=3)]
    assert names == ["uploads/t/changed.pdf", "uploads/t/new.pdf", "uploads/t/retry.pdf"]


def test_nothing_pending_leaves_the_cluster_alone() -> None:
    cluster = _Cluster()
    report = _sweep(_Store([_blob("uploads/t/a.pdf")], {"uploads/t/a.pdf": {"etag": "e1", "done": True, "attempts": 1}}),
                    cluster, _Ray())
    assert report == {"pending": 0, "succeeded": 0, "failed": 0} and cluster.events == []


def test_a_pass_resumes_ingests_per_tenant_records_state_and_suspends() -> None:
    store, cluster, ray = _Store([_blob("uploads/acme/a.pdf"), _blob("uploads/beta/b.pdf", "e2")]), _Cluster(), _Ray()
    report = _sweep(store, cluster, ray)
    assert report == {"pending": 2, "succeeded": 2, "failed": 0}
    assert cluster.events == ["resume", "ready", "suspend"]
    assert [(e.split()[-2:], env["TENANT_ID"]) for e, env, _ in ray.submitted] == [
        (["documents", "uploads/acme/a.pdf"], "acme"), (["documents", "uploads/beta/b.pdf"], "beta")]
    assert all(env["A"] == "1" for _, env, _ in ray.submitted)
    assert store.state["uploads/acme/a.pdf"] == {"etag": "e1", "done": True, "attempts": 1}
    assert store.saves == 2


def test_a_failed_blob_is_retried_later_and_does_not_stop_the_others() -> None:
    store = _Store([_blob("uploads/t/bad.pdf"), _blob("uploads/t/good.pdf")])
    cluster = _Cluster()
    report = _sweep(store, cluster, _Ray({"uploads/t/bad.pdf": "FAILED"}))
    assert report == {"pending": 2, "succeeded": 1, "failed": 1} and cluster.events[-1] == "suspend"
    assert store.state["uploads/t/bad.pdf"] == {"etag": "e1", "done": False, "attempts": 1}
    again = _sweep(store, _Cluster(), _Ray({"uploads/t/bad.pdf": "FAILED"}))
    assert again["pending"] == 1 and store.state["uploads/t/bad.pdf"]["attempts"] == 2


def test_the_cluster_is_suspended_even_when_it_never_becomes_ready() -> None:
    class _Slow(_Cluster):
        def wait_ready(self):
            raise RuntimeError("never ready")

    cluster = _Slow()
    try:
        _sweep(_Store([_blob("uploads/t/a.pdf")]), cluster, _Ray())
    except RuntimeError:
        pass
    assert cluster.events == ["resume", "suspend"]


def test_a_cluster_someone_else_is_running_is_not_suspended() -> None:
    cluster = _Cluster(owns=False)
    _sweep(_Store([_blob("uploads/t/a.pdf")]), cluster, _Ray())
    assert "suspend" not in cluster.events


def test_manifest_and_terraform_wire_the_sweeper() -> None:
    docs = list(yaml.safe_load_all((ROOT / "deploy/ray/ingest-sweeper-azure.yaml").read_text()))
    kinds = {d["kind"]: d for d in docs}
    cron = kinds["CronJob"]["spec"]
    assert cron["concurrencyPolicy"] == "Forbid" and cron["jobTemplate"]["spec"]["backoffLimit"] == 0
    pod = cron["jobTemplate"]["spec"]["template"]
    assert pod["metadata"]["labels"]["azure.workload.identity/use"] == "true"
    assert pod["spec"]["serviceAccountName"] == "ingest-sweeper"
    assert pod["spec"]["containers"][0]["image"] == "__INGESTION_IMAGE__"
    verbs = {(r["resources"][0], tuple(r["verbs"])) for r in kinds["Role"]["rules"]}
    assert verbs == {("rayclusters", ("get", "patch")), ("pods", ("get", "list"))}
    assert (ROOT / "pipelines/jobs/ray_job.yaml").exists()
    tf = (ROOT / "infra/terraform/azure/iam.tf").read_text()
    assert "system:serviceaccount:default:ingest-sweeper" in tf
    assert "ingest-sweeper-azure.yaml" in (ROOT / "scripts/deploy_ingestion_cluster_azure.sh").read_text()


def test_a_refused_connection_to_the_ray_head_is_retried(monkeypatch) -> None:
    import io
    import urllib.error

    from pipelines.ingestion import sweeper

    calls = {"n": 0}

    class _Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_urlopen(request, timeout=0):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.URLError(ConnectionRefusedError(111, "refused"))
        return _Response(b'{"status": "RUNNING"}')

    monkeypatch.setattr(sweeper.urllib.request, "urlopen", fake_urlopen)
    api = sweeper.RayJobApi(sleep=lambda _: None)
    assert api.status("x") == "RUNNING" and calls["n"] == 3


def test_a_ray_http_error_is_not_retried(monkeypatch) -> None:
    import urllib.error

    from pipelines.ingestion import sweeper

    calls = {"n": 0}

    def fake_urlopen(request, timeout=0):
        calls["n"] += 1
        raise urllib.error.HTTPError("u", 400, "bad", {}, None)

    monkeypatch.setattr(sweeper.urllib.request, "urlopen", fake_urlopen)
    try:
        sweeper.RayJobApi(sleep=lambda _: None).status("x")
    except urllib.error.HTTPError:
        pass
    assert calls["n"] == 1
