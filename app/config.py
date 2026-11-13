"""Runtime configuration, read from the environment once at import time.

The merge service keeps all state in memory: no database, no cross-request
cache of annotation lookups.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULTS = {
    # Upstream annotation source.
    "annotation_base_url": "http://annotation-source:8001",
    "annotation_timeout_seconds": 10.0,
    "annotation_max_connections": 100,
    # Upload limits.
    "max_upload_bytes": 32 * 1024 * 1024,
    "max_rows": 100_000,
    # Distinct genes one batch may look up concurrently. Lookups already in
    # flight for another batch are shared, never duplicated.
    "per_run_concurrency": 8,
    # Finished batches kept in memory (oldest finished evicted first).
    "max_retained_runs": 1000,
}


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class Settings:
    annotation_base_url: str
    annotation_timeout_seconds: float
    annotation_max_connections: int
    max_upload_bytes: int
    max_rows: int
    per_run_concurrency: int
    max_retained_runs: int

    @classmethod
    def from_env(cls) -> "Settings":
        d = DEFAULTS
        return cls(
            annotation_base_url=os.environ.get(
                "ANNOTATION_BASE_URL", d["annotation_base_url"]
            ).rstrip("/"),
            annotation_timeout_seconds=_float(
                "ANNOTATION_TIMEOUT_SECONDS", d["annotation_timeout_seconds"]
            ),
            annotation_max_connections=_int(
                "ANNOTATION_MAX_CONNECTIONS", d["annotation_max_connections"]
            ),
            max_upload_bytes=_int("MAX_UPLOAD_BYTES", d["max_upload_bytes"]),
            max_rows=_int("MAX_ROWS", d["max_rows"]),
            per_run_concurrency=_int(
                "PER_RUN_CONCURRENCY", d["per_run_concurrency"]
            ),
            max_retained_runs=_int("MAX_RETAINED_RUNS", d["max_retained_runs"]),
        )


SETTINGS = Settings.from_env()
