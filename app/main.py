"""FastAPI application: annotation batches, durable samples, count matrix.

Annotation runs are in-memory (upload, progress, cancellation, download);
samples and the published matrix live on the configured data volume and are
rebuilt/reloaded across restarts.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from . import __version__
from .config import SETTINGS
from .csvio import CsvValidationError, parse_gene_csv
from .hub import AnnotationHub
from .matrix import MatrixManager, MatrixNotFound
from .runs import (
    DownloadUnavailable,
    RunAlreadyFinished,
    RunManager,
    UnknownRun,
)
from .samples import (
    InvalidSampleId,
    SampleNotFound,
    SampleSaveError,
    SampleStore,
    SampleValidationError,
    parse_sample_csv,
    validate_sample_id,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("api")

DESCRIPTION = """\
Merges sequencing-software expression output onto one fixed human annotation
release (GRCh38 / Ensembl 110) before batches are combined, and assembles the
delivered per-sample counts into one count matrix.

Annotation batches
------------------
* Upload a CSV with a single `gene_id` column; the original column is preserved.
* Every row's identifier is normalised only for lookup by dropping a trailing
  `.<digits>` version suffix (`ENSG00000141510.18` -> `ENSG00000141510`).
* `symbol` and `chromosome` are appended asynchronously. Row order and
  duplicate rows are preserved exactly.
* A 404 from the annotation source marks that gene as `unmatched`; any other
  upstream failure fails the whole batch and no file is produced.
* Identical identifiers in flight are looked up once and shared by every batch
  that needs them. Cancelling one batch releases only its own interest.
* Annotation run state is in memory. Finished lookups are not cached.

Samples and the count matrix
----------------------------
* Upload `gene_id,count` CSVs per sample. Version suffixes are stripped; a
  duplicate gene within one sample rejects the whole upload. Counts must be
  non-negative integers.
* `PUT /samples/{sample_id}` creates or replaces the sample as a whole.
  Validation or save failures keep the previous sample; the stored version is
  only swapped after the new file is fully written.
* The matrix has genes as rows and samples as columns, both sorted by
  identifier. A gene missing from a sample is an empty cell, never `0`.
* Samples and the published matrix live on the data volume, so the original
  annotation endpoints keep working and storage survives a restart.
* Rebuilds run in the background. Samples can be uploaded and the last
  complete matrix downloaded while a rebuild is running. A build is only
  published when it still matches every current sample; stale builds are
  discarded and rebuilt.
* While samples are not yet reflected, status reports `stale` (or `building`);
  a failed build reports `failed` and the previous complete matrix, if any,
  remains downloadable.
"""


def _data_dir() -> Path:
    if SETTINGS.data_dir:
        path = Path(SETTINGS.data_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path
    # No durable location configured (tests, ad-hoc runs): use a temporary
    # directory instead of polluting the working tree.
    return Path(tempfile.mkdtemp(prefix="expression-merge-"))


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
    # Samples live on the data volume; the matrix is rebuilt from them. Both
    # are (re)loaded on every start so the annotation endpoints keep working
    # and an interrupted rebuild resumes.
    store = SampleStore(_data_dir())
    store.load()
    matrix = MatrixManager(
        store,
        debounce_seconds=SETTINGS.matrix_rebuild_debounce_seconds,
    )
    matrix.load()
    app.state.samples = store
    app.state.matrix = matrix
    matrix.start()
    try:
        yield
    finally:
        await matrix.close()
        await app.state.hub.close()
        if preset is None:
            await client.aclose()


def create_app(client: httpx.AsyncClient | None = None) -> FastAPI:
    app = FastAPI(
        title="Human Gene Annotation Merge & Count Matrix",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
        contact={"name": "Bioinformatics data team"},
        openapi_tags=[
            {
                "name": "batches",
                "description": "Annotate a gene_id CSV against the fixed GRCh38 "
                "release (in-memory runs).",
            },
            {
                "name": "samples",
                "description": "Store or correct one delivery's gene_id,count "
                "CSV on the data volume.",
            },
            {
                "name": "matrix",
                "description": "Status and download of the count matrix "
                "rebuilt from all current samples.",
            },
            {"name": "ops", "description": "Liveness."},
        ],
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

    # -- samples -------------------------------------------------------

    @app.get(
        "/samples",
        tags=["samples"],
        summary="List stored samples",
    )
    async def list_samples(request: Request) -> dict[str, object]:
        store: SampleStore = request.app.state.samples
        metas = await asyncio.to_thread(store.list_samples)
        samples = [meta.snapshot() for meta in metas]
        return {"count": len(samples), "samples": samples}

    @app.put(
        "/samples/{sample_id}",
        status_code=201,
        tags=["samples"],
        summary="Upload or replace one sample's gene_id,count CSV",
        response_class=JSONResponse,
    )
    async def put_sample(
        request: Request, sample_id: str, file: UploadFile = File(...)
    ) -> JSONResponse:
        try:
            validate_sample_id(sample_id)
        except InvalidSampleId as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        data = await file.read()
        if not data:
            raise HTTPException(status_code=400, detail="uploaded file is empty")
        if len(data) > SETTINGS.max_upload_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"file exceeds {SETTINGS.max_upload_bytes} bytes",
            )
        store: SampleStore = request.app.state.samples
        matrix: MatrixManager = request.app.state.matrix
        try:
            # Parsing and the fsynced write happen in a worker thread so a
            # concurrent matrix rebuild cannot stall request handling.
            parsed = await asyncio.to_thread(parse_sample_csv, data)
            if parsed.gene_count > SETTINGS.max_rows:
                raise HTTPException(
                    status_code=413,
                    detail=f"file exceeds {SETTINGS.max_rows} rows",
                )
            meta, changed, created = await asyncio.to_thread(
                store.replace, sample_id, parsed
            )
        except SampleValidationError as exc:
            # Nothing has been written: the previously stored sample (if any)
            # is untouched and still served.
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except SampleSaveError as exc:
            # The save failed; the previous sample is still the registered one.
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if changed:
            matrix.request_rebuild(reason=f"sample {sample_id} saved")
        status = matrix.status().snapshot()
        log.info(
            "sample %s %s (%d genes); matrix state=%s",
            sample_id,
            "created" if created else "replaced",
            meta.gene_count,
            status["state"],
        )
        return JSONResponse(
            status_code=201 if created else 200,
            content={
                "sample": meta.snapshot(),
                "created": created,
                "matrix": status,
            },
        )

    @app.get(
        "/samples/{sample_id}",
        tags=["samples"],
        summary="Metadata of one stored sample",
    )
    async def get_sample(request: Request, sample_id: str) -> dict[str, object]:
        store: SampleStore = request.app.state.samples
        try:
            meta = await asyncio.to_thread(store.get, sample_id)
        except SampleNotFound:
            raise HTTPException(status_code=404, detail=f"unknown sample {sample_id}")
        return meta.snapshot()

    @app.get(
        "/samples/{sample_id}/download",
        tags=["samples"],
        summary="Download the stored gene_id,count CSV of one sample",
        response_class=Response,
    )
    async def download_sample(request: Request, sample_id: str) -> Response:
        store: SampleStore = request.app.state.samples
        try:
            content = await asyncio.to_thread(store.read, sample_id)
        except SampleNotFound:
            raise HTTPException(status_code=404, detail=f"unknown sample {sample_id}")
        except SampleSaveError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return Response(
            content=content,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{sample_id}.csv"',
                "Cache-Control": "no-store",
            },
        )

    # -- matrix --------------------------------------------------------

    @app.get(
        "/matrix",
        tags=["matrix"],
        summary="Status of the count matrix",
    )
    async def matrix_status(request: Request) -> dict[str, object]:
        matrix: MatrixManager = request.app.state.matrix
        return matrix.status().snapshot()

    @app.get(
        "/matrix/download",
        tags=["matrix"],
        summary="Download the last complete count matrix",
        response_class=Response,
    )
    async def download_matrix(request: Request) -> Response:
        matrix: MatrixManager = request.app.state.matrix
        try:
            content = await asyncio.to_thread(matrix.read_matrix)
        except MatrixNotFound as exc:
            # No complete matrix exists yet (or the only build failed): the
            # body explains whether the rebuild is pending or failed.
            raise HTTPException(status_code=409, detail=exc.status) from exc
        return Response(
            content=content,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="count_matrix.csv"',
                # The status/document pair changes when a rebuild publishes;
                # never let a proxy serve an older matrix.
                "Cache-Control": "no-store",
                "X-Matrix-State": matrix.status().state.value,
            },
        )

    return app


app = create_app()
