"""In-memory batch runs: lifecycle, cancellation and progress accounting.

A run is created by an upload and processed by a background task that walks
its distinct identifiers through the shared :class:`~app.hub.AnnotationHub`.
All state lives in this process; nothing is persisted, and finished lookups
are not cached anywhere.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .csvio import ParsedBatch, render_output_csv
from .hub import AnnotationHub, AnnotationRequestFailed, BatchCancelled

log = logging.getLogger("runs")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class RunState(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self is not RunState.RUNNING


class DownloadUnavailable(Exception):
    """No CSV can be produced for this run (not yet, or never)."""


class UnknownRun(KeyError):
    pass


class RunAlreadyFinished(Exception):
    pass


@dataclass(slots=True)
class Run:
    id: str
    filename: str
    parsed: ParsedBatch
    state: RunState = RunState.RUNNING
    created_at: str = field(default_factory=_now)
    finished_at: str | None = None
    error: str | None = None
    output: bytes | None = None

    cancel_requested: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    # Outcome per normalised identifier; ``None`` means upstream answered 404.
    annotations: dict[str, dict[str, str] | None] = field(default_factory=dict)
    # Rows whose identifier has an outcome so far (progress follows the file).
    rows_done: int = 0
    matched_count: int = 0
    unmatched_count: int = 0

    @property
    def total_rows(self) -> int:
        return self.parsed.row_count

    def snapshot(self) -> dict[str, Any]:
        if self.state is RunState.COMPLETED:
            progress = 100.0
        elif self.total_rows:
            progress = round(100.0 * self.rows_done / self.total_rows, 1)
        else:  # pragma: no cover - the parser rejects empty files
            progress = 0.0
        return {
            "batch_id": self.id,
            "filename": self.filename,
            "state": self.state.value,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "rows_total": self.total_rows,
            "rows_processed": self.rows_done,
            "distinct_genes": len(self.parsed.distinct_lookup_ids),
            "matched_genes": self.matched_count,
            "unmatched_genes": self.unmatched_count,
            "progress_percent": progress,
            "download_available": self.state is RunState.COMPLETED,
            "error": self.error,
        }


class RunManager:
    """Owns every run in the process and the hub they share."""

    def __init__(self, hub: AnnotationHub, *, concurrency: int, retention: int) -> None:
        self.hub = hub
        self.concurrency = max(1, concurrency)
        self.retention = max(1, retention)
        self._runs: dict[str, Run] = {}
        self._order: list[str] = []

    # -- lifecycle -----------------------------------------------------

    def create(self, filename: str, parsed: ParsedBatch) -> Run:
        run = Run(id=f"run-{uuid.uuid4().hex[:12]}", filename=filename, parsed=parsed)
        self._runs[run.id] = run
        self._order.append(run.id)
        self._evict()
        run.task = asyncio.create_task(self._process(run))
        # Safety net: a batch cancelled before its coroutine ever ran would
        # otherwise keep finished_at unset and a stale task reference.
        run.task.add_done_callback(lambda _task, run=run: self._finalize(run))
        return run

    def get(self, run_id: str) -> Run:
        run = self._runs.get(run_id)
        if run is None:
            raise UnknownRun(run_id)
        return run

    def _evict(self) -> None:
        """Bound memory: drop the oldest finished runs beyond the retention cap."""
        while len(self._order) > self.retention:
            run_id = self._order[0]
            run = self._runs.get(run_id)
            if run is not None and not run.state.terminal:
                break  # never evict a live run; wait until it finishes
            self._order.pop(0)
            self._runs.pop(run_id, None)

    def cancel(self, run_id: str) -> Run:
        run = self.get(run_id)
        if run.state.terminal:
            raise RunAlreadyFinished(run_id)
        # Takes effect immediately, before the coroutine gets a chance to run:
        # the state is flipped here and the task is cancelled. Waiters on
        # shared queries are released at once without disturbing other runs.
        run.cancel_requested.set()
        run.state = RunState.CANCELLED
        if run.task is not None and not run.task.done():
            run.task.cancel()
        else:
            self._finalize(run)
        return run

    # -- processing ----------------------------------------------------

    async def _process(self, run: Run) -> None:
        cancelled = run.cancel_requested
        try:
            semaphore = asyncio.Semaphore(self.concurrency)

            async def resolve(gene: str) -> None:
                async with semaphore:
                    if cancelled.is_set():
                        raise BatchCancelled()
                    value = await self.hub.lookup(gene, cancelled)
                    # Discard late answers to a cancelled batch: it must not be
                    # resurrected and its data must not leak into the output.
                    if cancelled.is_set():
                        raise BatchCancelled()
                    run.annotations[gene] = value
                    run.rows_done += run.parsed.lookup_ids.count(gene)
                    if value is None:
                        run.unmatched_count += 1
                    else:
                        run.matched_count += 1

            tasks = [
                asyncio.create_task(resolve(gene))
                for gene in run.parsed.distinct_lookup_ids
            ]
            try:
                await _gather_cancellable(tasks, cancelled)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            # Last guard against a cancel that landed while tasks were
            # finishing: a cancelled batch must never become completed.
            if cancelled.is_set():
                raise BatchCancelled()
            run.output = render_output_csv(run.parsed, run.annotations)
            run.rows_done = run.total_rows
            run.state = RunState.COMPLETED
        except (BatchCancelled, asyncio.CancelledError):
            run.state = RunState.CANCELLED
            log.info(
                "batch %s cancelled after %d/%d rows",
                run.id, run.rows_done, run.total_rows,
            )
        except AnnotationRequestFailed as exc:
            run.state = RunState.FAILED
            run.error = f"annotation request failed: {exc}"
            log.warning("batch %s failed: %s", run.id, exc)
        except Exception as exc:  # defensive: never leave a run hanging
            run.state = RunState.FAILED
            run.error = f"unexpected error: {exc!r}"
            log.exception("batch %s crashed", run.id)
        finally:
            self._finalize(run)

    @staticmethod
    def _finalize(run: Run) -> None:
        """Close the run out exactly once (also covers tasks killed unstarted)."""
        if run.finished_at is not None:
            return
        run.finished_at = _now()
        run.task = None
        if run.state is not RunState.COMPLETED:
            # Neither a cancelled nor a failed batch produces output, and
            # partial lookups are dropped so nothing lingers in memory.
            run.output = None
            run.annotations.clear()

    # -- download ------------------------------------------------------

    def require_download(self, run: Run) -> bytes:
        if run.state is RunState.COMPLETED and run.output is not None:
            return run.output
        if run.state is RunState.CANCELLED:
            raise DownloadUnavailable("batch was cancelled; no CSV is produced")
        if run.state is RunState.FAILED:
            raise DownloadUnavailable(
                f"batch failed; no CSV is produced: {run.error}"
            )
        raise DownloadUnavailable(
            "batch is still running; download is available once it completes"
        )


async def _gather_cancellable(
    tasks: list[asyncio.Task[None]], cancelled: asyncio.Event
) -> None:
    """Wait for ``tasks`` but bail out promptly when ``cancelled`` is set.

    Waiting on the batch's own tasks alone is not enough: a shared query may
    legitimately outlive this batch, so we also race against the cancel event
    to make cancellation take effect immediately.
    """
    pending: set[asyncio.Task[None]] = set(tasks)
    while pending:
        if cancelled.is_set():
            raise BatchCancelled()
        cancel_wait = asyncio.create_task(cancelled.wait())
        try:
            done, _ = await asyncio.wait(
                pending | {cancel_wait}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            cancel_wait.cancel()
            await asyncio.gather(cancel_wait, return_exceptions=True)
        if cancel_wait in done:
            raise BatchCancelled()
        done.discard(cancel_wait)
        pending -= done
        for task in done:
            task.result()  # re-raise the first failure
