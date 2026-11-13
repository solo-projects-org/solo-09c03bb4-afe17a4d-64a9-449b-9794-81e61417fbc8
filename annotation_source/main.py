"""Entrypoint for the local, deliberately slow annotation source.

Run with ``uvicorn annotation_source.main:app``.  The source implements the
fixed human annotation release and an optional per-request delay so the
sharing/cancellation behaviour of the merge service is easy to observe.
"""

from __future__ import annotations

import os

from fastapi import FastAPI

from .service import build_router, load_reference


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def create_app() -> FastAPI:
    delay = _float("ANNOTATION_SOURCE_DELAY_SECONDS", 1.0)
    app = FastAPI(
        title="Local Human Gene Annotation Source",
        version="1.0.0",
        description=(
            "Fixed GRCh38 annotation endpoint used by the merge service. "
            f"Responses are delayed by {delay:g}s to emulate a slow upstream."
        ),
    )
    app.state.reference = load_reference(
        os.environ.get("ANNOTATION_DATA_DIR", "/data"),
        os.environ.get(
            "ANNOTATION_REFERENCE_FILE", "human_gene_annotation.GRCh38.tsv"
        ),
    )
    app.state.requests_total = 0
    app.state.requests_per_gene = {}
    app.include_router(build_router(delay_seconds=delay))
    return app


app = create_app()
