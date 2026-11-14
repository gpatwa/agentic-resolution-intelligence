"""Run-time options of the ingestion job that depend on the chosen providers (no Ray import, so they are testable)."""

from __future__ import annotations

from typing import Mapping


def int_env(env: Mapping[str, str], name: str, default: int) -> int:
    return int(env.get(name, default))


def embed_num_gpus(env: Mapping[str, str]) -> float:
    """GPU fraction per embedder actor: only the self-hosted ``ray`` embedder uses one; API providers need none."""
    default = 0.2 if (env.get("INGEST_EMBED_PROVIDER") or "ray").strip().lower() == "ray" else 0.0
    return float(env.get("INGEST_EMBED_NUM_GPUS", default))


def graph_num_gpus(env: Mapping[str, str]) -> float:
    """GPU fraction per graph actor: only the self-hosted ``ray`` LLM path assumes one."""
    default = 0.5 if (env.get("INGEST_GRAPH_PROVIDER") or "ray").strip().lower() == "ray" else 0.0
    return float(env.get("INGEST_GRAPH_NUM_GPUS", default))


def tenant_id(env: Mapping[str, str]) -> str:
    """The tenant every sink writes under: ``TENANT_ID`` (set by scripts/ingest_azure.sh and the job file), else
    ``default``. Without this the sinks wrote everything under ``default`` whatever the caller asked for."""
    return (env.get("TENANT_ID") or "").strip() or "default"
