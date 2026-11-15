"""Automatic ingestion: ingest what was uploaded since the last pass, with no manual step.

Runs as a Kubernetes CronJob (deploy/ray/ingest-sweeper-azure.yaml) in the same image as the Ray cluster. Each pass:

  1. lists the blobs under ``uploads/<tenant>/...`` in the documents container;
  2. compares each blob's ETag with the state blob (``_state/ingest-state.json`` in the same container) to find what is
     new or changed, ignoring blobs written in the last ``INGEST_QUIET_SECONDS`` (an upload may still be in progress);
  3. if anything is pending, resumes the (otherwise suspended) Ray cluster, submits one Ray job per blob with
     ``TENANT_ID`` taken from the path, waits for each, and records the ETag only for blobs that succeeded;
  4. suspends the Ray cluster again (always, when this pass resumed it).

A cron pass only runs while the cluster is up (it is stopped off-hours), so uploads made while it is stopped are picked up
by the first pass after the start. Failed blobs are retried on later passes, at most ``INGEST_MAX_ATTEMPTS`` times for
the same ETag; a re-upload (new ETag) starts again. Re-ingesting a file is idempotent: its identity is its blob path.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

UPLOAD_ROOT = "uploads/"
STATE_BLOB = "_state/ingest-state.json"
TENANT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
RESUMED_ANNOTATION = "ingest-sweeper/resumed"


@dataclass(frozen=True)
class BlobInfo:
    name: str
    etag: str
    last_modified: datetime


def tenant_of(name: str) -> Optional[str]:
    """``uploads/<tenant>/<file...>`` -> tenant, or None for a path that is not an upload (or has an unsafe tenant)."""
    if not name.startswith(UPLOAD_ROOT) or name.endswith("/"):
        return None
    parts = name.split("/")
    if len(parts) < 3 or not TENANT_PATTERN.match(parts[1]):
        return None
    return parts[1]


def find_pending(
    blobs: List[BlobInfo], state: Dict[str, Any], now: datetime, quiet_seconds: int, max_attempts: int
) -> List[BlobInfo]:
    """Blobs to ingest now: new or changed (ETag differs), or the same ETag that failed fewer than ``max_attempts`` times."""
    pending = []
    for blob in sorted(blobs, key=lambda b: b.name):
        if tenant_of(blob.name) is None:
            continue
        if (now - blob.last_modified).total_seconds() < quiet_seconds:
            continue
        entry = state.get(blob.name)
        if entry is None or entry.get("etag") != blob.etag:
            pending.append(blob)
        elif not entry.get("done") and entry.get("attempts", 0) < max_attempts:
            pending.append(blob)
    return pending


def job_env(base_env: Dict[str, str], tenant: str) -> Dict[str, str]:
    env = dict(base_env)
    env["TENANT_ID"] = tenant
    return env


def run_pass(
    *,
    store: Any,
    cluster: Any,
    ray: Any,
    base_env: Dict[str, str],
    container: str,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
    quiet_seconds: int = 120,
    max_attempts: int = 3,
    job_timeout: int = 2700,
    poll_seconds: int = 15,
) -> Dict[str, int]:
    """One sweep. ``store``, ``cluster`` and ``ray`` are the Blob, Kubernetes and Ray-job adapters (fakes in tests)."""
    state = store.load_state()
    pending = find_pending(store.list_uploads(), state, now(), quiet_seconds, max_attempts)
    report = {"pending": len(pending), "succeeded": 0, "failed": 0}
    if not pending:
        return report

    # A previous pass that died after resuming left the annotation: this pass owns the suspend too.
    owns_resume = cluster.resume()
    try:
        cluster.wait_ready()
        for blob in pending:
            tenant = tenant_of(blob.name) or "default"
            submission_id = f"sweep-{int(now().timestamp())}-{abs(hash(blob.name)) % 10**6}"
            entry = {"etag": blob.etag, "done": False, "attempts": 0}
            if state.get(blob.name, {}).get("etag") == blob.etag:
                entry["attempts"] = state[blob.name].get("attempts", 0)
            entry["attempts"] += 1
            status = "FAILED"
            try:
                ray.submit(
                    f"python /app/pipelines/ingestion/main.py {container} {blob.name}",
                    job_env(base_env, tenant),
                    submission_id,
                )
                status = _wait_for_job(ray, submission_id, sleep, job_timeout, poll_seconds)
            except Exception as exc:  # one bad blob must not stop the rest
                print(f"sweep: {blob.name}: {exc}")
            entry["done"] = status == "SUCCEEDED"
            state[blob.name] = entry
            store.save_state(state)  # after every blob, so a crash loses at most the one in flight
            report["succeeded" if entry["done"] else "failed"] += 1
            print(f"sweep: {blob.name} (tenant {tenant}) -> {status}")
    finally:
        if owns_resume:
            cluster.suspend()
    return report


def _wait_for_job(ray: Any, submission_id: str, sleep: Callable[[float], None], timeout: int, poll: int) -> str:
    waited = 0
    while waited <= timeout:
        status = (ray.status(submission_id) or "").upper()
        if status in ("SUCCEEDED", "FAILED", "STOPPED"):
            return status
        sleep(poll)
        waited += poll
    return "TIMED_OUT"


# --- real adapters ------------------------------------------------------------------------------------------------------


class AzureBlobStore:
    """Uploads and the state blob, with the pod's workload identity (no storage key)."""

    def __init__(self, account: str, container: str):
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net", credential=DefaultAzureCredential()
        )
        self.container = service.get_container_client(container)

    def list_uploads(self) -> List[BlobInfo]:
        return [
            BlobInfo(b.name, str(b.etag), b.last_modified)
            for b in self.container.list_blobs(name_starts_with=UPLOAD_ROOT)
        ]

    def load_state(self) -> Dict[str, Any]:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return json.loads(self.container.download_blob(STATE_BLOB).readall())
        except ResourceNotFoundError:
            return {}

    def save_state(self, state: Dict[str, Any]) -> None:
        self.container.upload_blob(STATE_BLOB, json.dumps(state, sort_keys=True), overwrite=True)


class KubeRayCluster:
    """Resume, wait for and suspend the RayCluster through the Kubernetes API with the pod's service account."""

    API = "https://kubernetes.default.svc"
    SA = "/var/run/secrets/kubernetes.io/serviceaccount"

    def __init__(self, name: str = "rag-ray-cluster", namespace: str = "default", workers: int = 1):
        self.name, self.namespace, self.workers = name, namespace, workers
        self._ctx = ssl.create_default_context(cafile=f"{self.SA}/ca.crt")

    def _request(self, method: str, path: str, body: Optional[dict] = None, patch: bool = False) -> Any:
        with open(f"{self.SA}/token") as handle:
            token = handle.read().strip()
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"{self.API}{path}", data=data, method=method)
        request.add_header("Authorization", f"Bearer {token}")
        if body is not None:
            request.add_header("Content-Type", "application/merge-patch+json" if patch else "application/json")
        with urllib.request.urlopen(request, context=self._ctx, timeout=30) as response:
            return json.loads(response.read() or b"{}")

    def _cluster_path(self) -> str:
        return f"/apis/ray.io/v1/namespaces/{self.namespace}/rayclusters/{self.name}"

    def resume(self) -> bool:
        """Un-suspend with ``workers`` workers; True when this pass (or a dead earlier one) owns the later suspend."""
        current = self._request("GET", self._cluster_path())
        annotations = current.get("metadata", {}).get("annotations") or {}
        owns = bool(current.get("spec", {}).get("suspend")) or RESUMED_ANNOTATION in annotations
        patch = {
            "metadata": {"annotations": {RESUMED_ANNOTATION: "true"}} if owns else {},
            "spec": {"suspend": False},
        }
        self._request("PATCH", self._cluster_path(), patch, patch=True)
        groups = current["spec"]["workerGroupSpecs"]
        groups[0]["replicas"] = self.workers  # a JSON merge patch replaces lists whole
        self._request("PATCH", self._cluster_path(), {"spec": {"workerGroupSpecs": groups}}, patch=True)
        return owns

    def suspend(self) -> None:
        current = self._request("GET", self._cluster_path())
        groups = current["spec"]["workerGroupSpecs"]
        groups[0]["replicas"] = 0
        self._request(
            "PATCH",
            self._cluster_path(),
            {
                "metadata": {"annotations": {RESUMED_ANNOTATION: None}},
                "spec": {"suspend": True, "workerGroupSpecs": groups},
            },
            patch=True,
        )

    def wait_ready(self, timeout: int = 1800) -> None:
        selector = f"ray.io/cluster={self.name}"
        deadline = time.time() + timeout
        while time.time() < deadline:
            pods = self._request("GET", f"/api/v1/namespaces/{self.namespace}/pods?labelSelector={selector}")
            ready = {"head": 0, "worker": 0}
            for pod in pods.get("items", []):
                kind = pod["metadata"]["labels"].get("ray.io/node-type")
                conditions = {c["type"]: c["status"] for c in pod.get("status", {}).get("conditions", [])}
                if kind in ready and conditions.get("Ready") == "True":
                    ready[kind] += 1
            if ready["head"] >= 1 and ready["worker"] >= 1:
                return
            time.sleep(10)
        raise RuntimeError("the Ray head and a worker did not become Ready in time")


class RayJobApi:
    """Ray's job submission REST API on the head service."""

    def __init__(self, address: str = "http://rag-ray-cluster-head-svc:8265", *, attempts: int = 18, wait: float = 10.0,
                 sleep: Callable[[float], None] = time.sleep):
        self.address = address.rstrip("/")
        self.attempts, self.wait, self._sleep = attempts, wait, sleep

    def _call(self, method: str, path: str, body: Optional[dict] = None) -> Any:
        """The head pod can be Ready a moment before its dashboard and the service accept connections, so a refused
        connection is retried (about three minutes) instead of failing the blob."""
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(1, self.attempts + 1):
            request = urllib.request.Request(f"{self.address}{path}", data=data, method=method)
            if body is not None:
                request.add_header("Content-Type", "application/json")
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    return json.loads(response.read() or b"{}")
            except urllib.error.URLError as exc:
                if isinstance(exc, urllib.error.HTTPError) or attempt == self.attempts:
                    raise
                self._sleep(self.wait)

    def submit(self, entrypoint: str, env_vars: Dict[str, str], submission_id: str) -> None:
        self._call(
            "POST",
            "/api/jobs/",
            {"entrypoint": entrypoint, "submission_id": submission_id, "runtime_env": {"env_vars": env_vars}},
        )

    def status(self, submission_id: str) -> str:
        return self._call("GET", f"/api/jobs/{submission_id}").get("status", "")


def main() -> int:
    import yaml

    job_file = os.environ.get("INGEST_JOB_FILE", "/app/pipelines/jobs/ray_job.yaml")
    with open(job_file) as handle:
        base_env = {k: str(v) for k, v in yaml.safe_load(handle)["runtime_env"]["env_vars"].items()}
    account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME") or base_env["AZURE_STORAGE_ACCOUNT_NAME"]
    container = os.environ.get("INGEST_CONTAINER", "documents")
    report = run_pass(
        store=AzureBlobStore(account, container),
        cluster=KubeRayCluster(),
        ray=RayJobApi(),
        base_env=base_env,
        container=container,
        quiet_seconds=int(os.environ.get("INGEST_QUIET_SECONDS", "120")),
        max_attempts=int(os.environ.get("INGEST_MAX_ATTEMPTS", "3")),
    )
    print(f"sweep: {report}")
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
