"""Count-matrix rebuilds: build off to the side, then publish atomically.

A build reads every registered sample, writes a complete matrix into a
temporary file, and only then swaps it into place with :func:`os.replace`.
The published matrix is therefore always a complete file; a crash at any
point leaves the previous matrix (or no matrix), never a partial one.

Currency rules
--------------
Each build records the ``(sample_id, content_hash)`` set it started from.
Right before publishing, that snapshot is compared with the store again while
the store lock is held; if it changed, the build is stale, its temporary files
are discarded and a new build is scheduled for the newer state.  Rebuild
requests arriving while a build runs collapse into one "dirty" flag, so uploads
never wait for a build and a burst of corrections costs a single rebuild.

Startup always re-checks the data volume: leftover temporaries are deleted and
the published matrix is trusted only when its manifest hash matches the file.
A rebuild is then scheduled unless the samples on disk are exactly the ones
the published matrix was built from, which also resumes an interrupted rebuild.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .samples import SampleConflict, SampleStore, parse_sample_csv

log = logging.getLogger("matrix")

#: Single commit record: points at the immutable published matrix file. Only
#: replacing this file publishes a new matrix.
PUBLISHED_FILENAME = "published.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class MatrixState(str, Enum):
    #: No matrix has ever been published and none is being built.
    NONE = "none"
    #: A build is running (or about to start).
    BUILDING = "building"
    #: The published matrix reflects every current sample.
    CURRENT = "current"
    #: The published matrix is complete but does not include the latest
    #: samples yet; it is still served as the last complete matrix.
    STALE = "stale"
    #: The last build failed; the previous matrix (if any) is still served
    #: and the status says so explicitly.
    FAILED = "failed"


@dataclass(slots=True)
class BuildResult:
    built_at: str
    duration_seconds: float
    columns: int
    rows: int
    sources: tuple[tuple[str, str], ...]
    content_hash: str


@dataclass(slots=True)
class MatrixStatus:
    state: MatrixState
    built_at: str | None = None
    error: str | None = None
    build_id: str | None = None
    duration_seconds: float | None = None
    columns: int | None = None
    rows: int | None = None
    content_hash: str | None = None
    source_samples: list[str] = field(default_factory=list)
    #: Samples present now that are not reflected in the published matrix.
    pending_samples: list[str] = field(default_factory=list)
    #: Whether some complete matrix file has been published.
    has_matrix: bool = False

    @property
    def download_available(self) -> bool:
        """The last complete matrix stays downloadable while it is rebuilt."""
        if self.state is MatrixState.CURRENT:
            return True
        return self.has_matrix and self.state in (
            MatrixState.STALE,
            MatrixState.BUILDING,
            MatrixState.FAILED,
        )

    @property
    def stale(self) -> bool:
        """True when the served matrix does not reflect all current samples."""
        return self.state is not MatrixState.CURRENT

    def snapshot(self) -> dict[str, Any]:
        if self.state is MatrixState.CURRENT:
            note = "up to date"
        elif self.state is MatrixState.STALE:
            note = "stale: newer samples are not included yet"
        elif self.state is MatrixState.BUILDING:
            note = "building: the matrix is being rebuilt"
        elif self.state is MatrixState.FAILED:
            note = f"failed: {self.error}"
            if self.has_matrix:
                note += " (the last complete matrix is still available)"
        else:
            note = "no matrix has been built yet"
        return {
            "state": self.state.value,
            "status": note,
            "stale": self.stale,
            "download_available": self.download_available,
            "built_at": self.built_at,
            "build_id": self.build_id,
            "duration_seconds": self.duration_seconds,
            "columns": self.columns,
            "rows": self.rows,
            "source_samples": self.source_samples,
            "pending_samples": self.pending_samples,
            "error": self.error,
        }


class MatrixNotFound(Exception):
    """No complete matrix is available to serve."""

    def __init__(self, status: dict[str, Any]) -> None:
        super().__init__(status.get("status", "no matrix available"))
        self.status = status


def render_matrix(samples: list[tuple[str, dict[str, int]]]) -> tuple[bytes, int]:
    """Render genes as rows, samples as columns, both sorted by identifier.

    A gene missing from a sample is an empty cell. It is deliberately *not*
    rendered as ``0``: a missing value is not a zero count.
    """
    genes: set[str] = set()
    for _, columns in samples:
        genes.update(columns)
    ordered_genes = sorted(genes)
    ordered_samples = sorted(samples, key=lambda item: item[0])

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["gene_id", *(name for name, _ in ordered_samples)])
    for gene in ordered_genes:
        row = [gene]
        for _, columns in ordered_samples:
            count = columns.get(gene)
            row.append("" if count is None else str(count))
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8"), len(ordered_genes)


class MatrixManager:
    """Owns the published matrix, its rebuild loop and its status."""

    def __init__(
        self,
        store: SampleStore,
        *,
        debounce_seconds: float = 0.05,
    ) -> None:
        self.store = store
        self.matrix_dir = store.data_dir / "matrix"
        self.published_path = self.matrix_dir / PUBLISHED_FILENAME
        self.debounce_seconds = max(0.0, debounce_seconds)

        # Builds run in a worker thread, so status is guarded by a plain
        # threading lock (critical sections are tiny; there is no awaiting).
        self._state_lock = threading.RLock()
        self._status = MatrixStatus(state=MatrixState.NONE)
        #: Sources (sample id, content hash) of the published matrix.
        self._served: tuple[tuple[str, str], ...] | None = None
        self._dirty = False
        self._build_task: asyncio.Task[None] | None = None
        self._closed = False

    # -- startup -------------------------------------------------------

    def load(self) -> None:
        """Inspect the data volume and adopt any complete published matrix."""
        self.matrix_dir.mkdir(parents=True, exist_ok=True)
        # Anything named *.tmp belongs to an interrupted write. It was never
        # registered, so it must never be served.
        for leftover in self.matrix_dir.glob("*.tmp"):
            leftover.unlink(missing_ok=True)

        result = self._read_published()
        if result is None:
            self._remove_published()
            return
        self._served = result.sources
        self._status = MatrixStatus(
            state=MatrixState.CURRENT,
            built_at=result.built_at,
            duration_seconds=result.duration_seconds,
            columns=result.columns,
            rows=result.rows,
            content_hash=result.content_hash,
            source_samples=[name for name, _ in result.sources],
            has_matrix=True,
        )
        self._cleanup_unreferenced()
        log.info(
            "found published matrix from %s (%d genes x %d samples)",
            result.built_at,
            result.rows,
            result.columns,
        )

    def _published_record(self) -> dict[str, Any] | None:
        try:
            payload = json.loads(self.published_path.read_text("utf-8"))
        except (OSError, ValueError):
            return None
        return payload if isinstance(payload, dict) else None

    def _read_published(self) -> BuildResult | None:
        """Return the published build, or None if it is missing/unverifiable."""
        payload = self._published_record()
        if payload is None:
            if self.published_path.exists():
                log.warning("published matrix record is unreadable; rebuilding")
            return None
        try:
            matrix_file = self.matrix_dir / str(payload["file"])
            digest = hashlib.sha256(matrix_file.read_bytes()).hexdigest()
            if digest != payload["content_hash"]:
                log.warning("published matrix does not match its record; rebuilding")
                return None
            sources = tuple(
                (str(name), str(sample_hash))
                for name, sample_hash in payload["sources"]
            )
            return BuildResult(
                built_at=str(payload["built_at"]),
                duration_seconds=float(payload["duration_seconds"]),
                columns=int(payload["columns"]),
                rows=int(payload["rows"]),
                sources=sources,
                content_hash=str(payload["content_hash"]),
            )
        except (OSError, ValueError, KeyError, TypeError):
            log.warning("could not read published matrix record; rebuilding", exc_info=True)
            return None

    def _remove_published(self) -> None:
        # The record is the registration, so an unverifiable record simply
        # means "rebuild". Nothing is ever served from an unreferenced file.
        self.published_path.unlink(missing_ok=True)
        self._served = None
        self._cleanup_unreferenced()

    def _cleanup_unreferenced(self) -> None:
        """Best-effort removal of old build files after a successful publish."""
        record = self._published_record()
        keep = record.get("file") if record else None
        for path in self.matrix_dir.glob("*.csv"):
            if path.name != keep:
                path.unlink(missing_ok=True)

    # -- lifecycle -----------------------------------------------------

    def start(self) -> None:
        """Schedule the initial rebuild (resumes an interrupted one)."""
        self.request_rebuild(reason="startup")

    def request_rebuild(
        self, *, reason: str = "samples changed", force: bool = False
    ) -> None:
        """Mark the published matrix as lagging and make sure a build runs.

        Called after every successful sample save and at startup.  No build is
        scheduled when the published matrix already covers exactly the current
        samples, unless ``force`` is set.
        """
        if self._closed:
            return
        sources, _ = self.store.source_state()
        with self._state_lock:
            if not force and not sources and not self._served:
                # Nothing has ever been delivered and nothing is published:
                # there is no matrix to build, so report "none", not "building".
                return
            if (
                not force
                and self._status.state is MatrixState.CURRENT
                and self._served == sources
            ):
                return
            self._dirty = True
            if self._status.state is MatrixState.CURRENT:
                self._status.state = MatrixState.STALE
            elif self._status.state is MatrixState.NONE:
                self._status.state = MatrixState.BUILDING
            # BUILDING/STALE/FAILED stay as they are: a failure remains
            # visible until a retry actually succeeds.
        if self._build_task is None or self._build_task.done():
            self._build_task = asyncio.create_task(self._run())
            log.debug("matrix rebuild scheduled (%s)", reason)

    async def close(self) -> None:
        self._closed = True
        task = self._build_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    # -- status / download --------------------------------------------

    def status(self) -> MatrixStatus:
        with self._state_lock:
            current = self._status
            snapshot = MatrixStatus(
                state=current.state,
                built_at=current.built_at,
                error=current.error,
                build_id=current.build_id,
                duration_seconds=current.duration_seconds,
                columns=current.columns,
                rows=current.rows,
                content_hash=current.content_hash,
                source_samples=list(current.source_samples),
                has_matrix=current.has_matrix,
            )
            covered = {name for name, _ in self._served or ()}
        sources, _ = self.store.source_state()
        now = {name for name, _ in sources}
        snapshot.pending_samples = sorted(now - covered)
        return snapshot

    def read_matrix(self) -> bytes:
        status = self.status()
        if not status.download_available:
            raise MatrixNotFound(status.snapshot())
        # A publish may retire the file between reading the record and
        # opening the file; retry once against the freshly published record.
        for _ in range(2):
            record = self._published_record()
            if record is None:
                raise MatrixNotFound(status.snapshot())
            try:
                content = (self.matrix_dir / str(record["file"])).read_bytes()
            except (OSError, KeyError):
                continue
            expected = record.get("content_hash")
            if expected is not None and hashlib.sha256(content).hexdigest() != expected:
                # The file does not match the record that references it:
                # never serve bytes of unknown provenance.
                raise MatrixNotFound(status.snapshot())
            return content
        raise MatrixNotFound(status.snapshot())

    # -- the rebuild loop ---------------------------------------------

    async def _run(self) -> None:
        # A short debounce lets several corrections arriving together share a
        # single rebuild.
        if self.debounce_seconds:
            await asyncio.sleep(self.debounce_seconds)
        stale_attempts = 0
        while not self._closed:
            self._dirty = False
            try:
                # File IO and CSV parsing happen off the event loop so uploads
                # and downloads stay responsive during a rebuild.
                published = await asyncio.to_thread(self._build_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # never let the loop die
                log.warning("matrix rebuild failed: %s", exc)
                self._fail(str(exc))
                if self._dirty:
                    continue
                return
            if published:
                if self._dirty:
                    stale_attempts = 0
                    continue  # samples changed during the build: rebuild
                return
            # The build was stale: samples changed while it was reading them.
            stale_attempts = 0 if self._dirty else stale_attempts + 1
            if stale_attempts >= 3:
                log.warning(
                    "matrix rebuild keeps racing sample uploads; leaving the "
                    "published matrix marked stale until the next upload"
                )
                return
            if self.debounce_seconds:
                await asyncio.sleep(self.debounce_seconds)

    def _fail(self, message: str) -> None:
        with self._state_lock:
            self._status.state = MatrixState.FAILED
            self._status.error = message
            self._status.build_id = None

    def _build_once(self) -> bool:
        """Build and publish one matrix; return False when the build was stale."""
        build_id = f"build-{uuid.uuid4().hex[:12]}"
        started = time.monotonic()
        with self._state_lock:
            self._status.state = MatrixState.BUILDING
            self._status.build_id = build_id
            self._status.error = None

        sources, _ = self.store.source_state()
        if not sources:
            # Nothing delivered yet: there is no matrix to serve.
            self._remove_published()
            with self._state_lock:
                self._status = MatrixStatus(state=MatrixState.NONE)
            log.info("matrix build %s: no samples, nothing published", build_id)
            return True

        samples: list[tuple[str, dict[str, int]]] = []
        issue: tuple[MatrixState, str] | None = None
        for sample_id, sample_hash in sources:
            try:
                raw = self.store.read_verified(sample_id, sample_hash)
            except SampleConflict as exc:
                log.info("matrix build %s: %s", build_id, exc)
                issue = (MatrixState.STALE, str(exc))
                break
            except Exception as exc:  # unreadable sample -> nothing published
                log.warning(
                    "matrix build %s: cannot read %s: %s", build_id, sample_id, exc
                )
                issue = (
                    MatrixState.FAILED,
                    f"could not read sample {sample_id}: {exc}",
                )
                break
            try:
                parsed = parse_sample_csv(raw)
            except Exception as exc:
                log.warning(
                    "matrix build %s: invalid sample %s: %s",
                    build_id,
                    sample_id,
                    exc,
                )
                issue = (
                    MatrixState.FAILED,
                    f"sample {sample_id} is invalid: {exc}",
                )
                break
            samples.append((sample_id, parsed.columns))

        if issue is not None:
            state, message = issue
            with self._state_lock:
                self._status.state = state
                self._status.error = message
                self._status.build_id = None
            if state is MatrixState.FAILED:
                log.warning("matrix build %s failed: %s", build_id, message)
                return True
            return False

        content, rows = render_matrix(samples)
        content_hash = hashlib.sha256(content).hexdigest()
        result = BuildResult(
            built_at=_now(),
            duration_seconds=round(time.monotonic() - started, 4),
            columns=len(samples),
            rows=rows,
            sources=sources,
            content_hash=content_hash,
        )

        tmp_matrix = self.matrix_dir / f"{build_id}.{uuid.uuid4().hex}.tmp"
        tmp_record = self.matrix_dir / f"{build_id}.{uuid.uuid4().hex}.json.tmp"
        build_name = f"{build_id}.csv"
        try:
            self.matrix_dir.mkdir(parents=True, exist_ok=True)
            # Write and flush the complete matrix off to the side first.
            with open(tmp_matrix, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            record = json.dumps(
                {
                    "file": build_name,
                    "built_at": result.built_at,
                    "duration_seconds": result.duration_seconds,
                    "columns": result.columns,
                    "rows": result.rows,
                    "content_hash": result.content_hash,
                    "sources": [list(item) for item in result.sources],
                },
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            with open(tmp_record, "wb") as handle:
                handle.write(record)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            tmp_matrix.unlink(missing_ok=True)
            tmp_record.unlink(missing_ok=True)
            raise OSError(f"could not write matrix: {exc}") from exc

        # Currency check: publish only if the samples did not change while
        # this build was reading them. The store lock is held across the check
        # and the commit, so an upload cannot slip in between.
        with self.store.transaction():
            latest, _ = self.store.source_state()
            if latest != sources:
                tmp_matrix.unlink(missing_ok=True)
                tmp_record.unlink(missing_ok=True)
                with self._state_lock:
                    self._status.state = MatrixState.STALE
                    self._status.build_id = None
                log.info(
                    "matrix build %s discarded: samples changed during the "
                    "build; rescheduling",
                    build_id,
                )
                return False
            try:
                # The matrix gets its final name first; the single commit is
                # the atomic rename of the record. If this fails, the previous
                # record still points at the previous complete matrix, which
                # was never touched.
                os.replace(tmp_matrix, self.matrix_dir / build_name)
                os.replace(tmp_record, self.published_path)
            except OSError as exc:
                tmp_matrix.unlink(missing_ok=True)
                tmp_record.unlink(missing_ok=True)
                (self.matrix_dir / build_name).unlink(missing_ok=True)
                raise OSError(f"could not publish matrix: {exc}") from exc
            # Registration only happens after the file is in place.
            with self._state_lock:
                self._served = sources
                self._status = MatrixStatus(
                    state=MatrixState.CURRENT,
                    built_at=result.built_at,
                    build_id=build_id,
                    duration_seconds=result.duration_seconds,
                    columns=result.columns,
                    rows=result.rows,
                    content_hash=result.content_hash,
                    source_samples=[name for name, _ in sources],
                    has_matrix=True,
                )
            self._cleanup_unreferenced()

        log.info(
            "matrix build %s published: %d genes x %d samples in %.3fs",
            build_id,
            result.rows,
            result.columns,
            result.duration_seconds,
        )
        return True
