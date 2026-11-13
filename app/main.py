"""FastAPI application: upload, progress, cancellation, download."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from . import __version__
from .config import SETTINGS
from .csvio import CsvValidationError, parse_gene_csv
from .hub import AnnotationHub
from .runs import (
    DownloadUnavailable,
    RunAlreadyFinished,
    RunManager,
    UnknownRun,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("api")

DESCRIPTION = """\
Merges sequencing-software expression output onto one fixed human annotation
release (GRCh38 / Ensembl 110) before batches are combined.

* Upload a CSV with a single `gene_id` column; the original column is preserved.
* Every row's identifier is normalised only for lookup by dropping a trailing
  `.<digits>` version suffix (`ENSG00000141510.18` -> `ENSG00000141510`).
* `symbol` and `chromosome` are appended asynchronously. Row order and
  duplicate rows are preserved exactly.
* A 404 from the annotation source marks that gene as `unmatched`; any other
  upstream failure fails the whole batch and no file is produced.
* Identical identifiers in flight are looked up once and shared by every batch
  that needs them. Cancelling one batch releases only its own interest.
* All state is in memory. Finished lookups are not cached.
"""


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    preset: httpx.AsyncClient | None = getattr(app.state, "client", None)
    client = preset or httpx.AsyncClient(
        base_url=SETTINGS.annotation_base_url,
        timeout=SETTINGS.annotation_timeout_seconds,
        limits=httpx.Limits(
            max_connections=SETTINGS.annotation_max_connections,
            max_keepalive_connections=SETTINGS.annotation_max_connections,
        ),
    )
    app.state.hub = AnnotationHub(client)
    app.state.runs = RunManager(
        app.state.hub,
        concurrency=SETTINGS.per_run_concurrency,
        retention=SETTINGS.max_retained_runs,
    )
    try:
        yield
    finally:
        await app.state.hub.close()
        if preset is None:
            await client.aclose()


def create_app(client: httpx.AsyncClient | None = None) -> FastAPI:
    app = FastAPI(
        title="Human Gene Annotation Merge",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
        contact={"name": "Bioinformatics data team"},
    )
    if client is not None:
        app.state.client = client

    @app.get("/healthz", tags=["ops"], summary="Liveness")
    async def healthz(request: Request) -> dict[str, object]:
        hub: AnnotationHub = request.app.state.hub
        return {"status": "ok", "in_flight_lookups": hub.in_flight}

    @app.post(
        "/batches",
        status_code=202,
        tags=["batches"],
        summary="Upload a gene_id CSV and start annotation",
        response_class=JSONResponse,
    )
    async def create_batch(
        request: Request, file: UploadFile = File(...)
    ) -> JSONResponse:
        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="uploaded file is empty")
        if len(data) > SETTINGS.max_upload_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"file exceeds {SETTINGS.max_upload_bytes} bytes",
            )
        try:
            parsed = parse_gene_csv(data)
        except CsvValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if parsed.row_count > SETTINGS.max_rows:
            raise HTTPException(
                status_code=413,
                detail=f"file exceeds {SETTINGS.max_rows} rows",
            )

        manager: RunManager = request.app.state.runs
        run = manager.create(file.filename or "genes.csv", parsed)
        log.info("batch %s accepted (%d rows)", run.id, run.total_rows)
        return JSONResponse(status_code=202, content=run.snapshot())

    @app.get(
        "/batches/{batch_id}",
        tags=["batches"],
        summary="Progress of a batch",
    )
    async def get_batch(request: Request, batch_id: str) -> dict[str, object]:
        manager: RunManager = request.app.state.runs
        try:
            run = manager.get(batch_id)
        except UnknownRun:
            raise HTTPException(status_code=404, detail=f"unknown batch {batch_id}")
        return run.snapshot()

    @app.post(
        "/batches/{batch_id}/cancel",
        status_code=202,
        tags=["batches"],
        summary="Cancel a running batch (takes effect immediately)",
    )
    async def cancel_batch(request: Request, batch_id: str) -> dict[str, object]:
        manager: RunManager = request.app.state.runs
        try:
            run = manager.cancel(batch_id)
        except UnknownRun:
            raise HTTPException(status_code=404, detail=f"unknown batch {batch_id}")
        except RunAlreadyFinished:
            raise HTTPException(
                status_code=409,
                detail=f"batch {batch_id} already finished; it cannot be cancelled",
            )
        return run.snapshot()

    @app.get(
        "/batches/{batch_id}/download",
        tags=["batches"],
        summary="Download the annotated CSV of a completed batch",
        response_class=Response,
    )
    async def download_batch(request: Request, batch_id: str) -> Response:
        manager: RunManager = request.app.state.runs
        try:
            run = manager.get(batch_id)
        except UnknownRun:
            raise HTTPException(status_code=404, detail=f"unknown batch {batch_id}")
        try:
            content = manager.require_download(run)
        except DownloadUnavailable as exc:
            # No file exists for running/cancelled/failed batches.
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        filename = f"{run.filename.rsplit('.', 1)[0] or 'genes'}.annotated.csv"
        return Response(
            content=content,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                # Lookups are never cached; don't let proxies cache the file
                # of a run whose state could be evicted.
                "Cache-Control": "no-store",
            },
        )

    return app


app = create_app()
