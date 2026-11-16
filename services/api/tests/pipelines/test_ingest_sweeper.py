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
    assert ("rayclusters", ("get", "patch")) in verbs and ("pods", ("get", "list")) in verbs
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


# ----- the ingestion lock ------------------------------------------------------------------------------------------------


class _FakeKube:
    """Just enough of the Lease API: POST 409s when it exists, PUT replaces, DELETE removes."""

    def __init__(self, lease=None):
        self.lease, self.calls = lease, []

    def __call__(self, method, path, body=None, patch=False):
        import urllib.error

        self.calls.append(method)
        if method == "POST":
            if self.lease is not None:
                raise urllib.error.HTTPError(path, 409, "exists", {}, None)
            self.lease = body
            return body
        if self.lease is None:
            raise urllib.error.HTTPError(path, 404, "gone", {}, None)
        if method == "GET":
            return {**self.lease}
        if method == "PUT":
            self.lease = body
            return body
        if method == "DELETE":
            self.lease = None
            return {}
        raise AssertionError(method)


def _lease(kube, holder, now=NOW):
    from pipelines.ingestion.sweeper import KubeLease

    return KubeLease(kube, holder, now=lambda: now)


def test_a_free_lease_is_taken_and_released() -> None:
    kube = _FakeKube()
    lock = _lease(kube, "a")
    assert lock.acquire() and kube.lease["spec"]["holderIdentity"] == "a"
    lock.release()
    assert kube.lease is None


def test_a_lease_held_by_someone_else_is_not_taken_until_it_expires() -> None:
    kube = _FakeKube()
    assert _lease(kube, "a").acquire()
    assert not _lease(kube, "b", NOW + timedelta(seconds=600)).acquire()
    assert kube.lease["spec"]["holderIdentity"] == "a"
    assert _lease(kube, "b", NOW + timedelta(seconds=1300)).acquire()  # past leaseDurationSeconds (1200)
    assert kube.lease["spec"]["holderIdentity"] == "b"


def test_only_the_holder_can_release_and_a_lost_lease_is_noticed_on_renew() -> None:
    kube = _FakeKube()
    a = _lease(kube, "a")
    a.acquire()
    _lease(kube, "b").release()
    assert kube.lease is not None
    kube.lease["spec"]["holderIdentity"] = "b"
    try:
        a.renew()
        raise AssertionError("renew should fail")
    except RuntimeError:
        pass


def test_a_pass_that_cannot_get_the_lock_does_nothing() -> None:
    class _Busy:
        def acquire(self):
            return False

        def release(self):
            raise AssertionError("must not release a lock it never held")

    cluster = _Cluster()
    report = _sweep(_Store([_blob("uploads/t/a.pdf")]), cluster, _Ray(), lock=_Busy())
    assert report["skipped"] == 1 and report["succeeded"] == 0 and cluster.events == []


def test_a_pass_holds_the_lock_while_it_runs_and_releases_it_even_on_failure() -> None:
    log = []

    class _Lock:
        def acquire(self):
            log.append("acquire")
            return True

        def renew(self):
            log.append("renew")

        def release(self):
            log.append("release")

    class _Slow(_Cluster):
        def wait_ready(self):
            raise RuntimeError("boom")

    try:
        _sweep(_Store([_blob("uploads/t/a.pdf")]), _Slow(), _Ray(), lock=_Lock())
    except RuntimeError:
        pass
    assert log == ["acquire", "release"]
    log.clear()
    _sweep(_Store([_blob("uploads/t/a.pdf")]), _Cluster(), _Ray(), lock=_Lock())
    assert log[0] == "acquire" and "renew" in log and log[-1] == "release"


def test_the_manual_script_and_the_role_use_the_same_lease() -> None:
    script = (ROOT / "scripts/ingest_azure.sh").read_text()
    assert "kind: Lease" in script and "name: ingest-sweeper" in script and "acquire_lease || exit 1" in script
    role = [d for d in yaml.safe_load_all((ROOT / "deploy/ray/ingest-sweeper-azure.yaml").read_text()) if d["kind"] == "Role"][0]
    lease_rules = [r for r in role["rules"] if r["resources"] == ["leases"]]
    assert {v for r in lease_rules for v in r["verbs"]} == {"create", "get", "update", "delete"}
