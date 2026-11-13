"""Shared, single-flight annotation lookups with per-batch cancellation.

Design notes
------------
* A *query* is one upstream lookup keyed by the version-suffix-stripped
  identifier.  Every batch that needs that identifier while it is in flight
  attaches a *waiter* to the same query, so N batches mentioning
  ``ENSG00000141510`` cause exactly one HTTP request.
* Waiters let one batch leave without touching the others: cancelling a batch
  only releases its own waiter.  The query is aborted only when its last
  waiter leaves before the upstream answered.
* Nothing survives a finished query.  A later batch re-queries the upstream.
  Registry entries are retired by object identity, so cleanup belonging to an
  old request can neither remove nor revive a newer query for the same gene.
* A cancelled query's in-flight HTTP task is cancelled; if its response was
  already in transit, its outcome is dropped because the query object is no
  longer registered and has no waiters left.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger("annotation_hub")


class BatchCancelled(Exception):
    """Raised inside a batch coroutine when that batch was cancelled."""


class AnnotationRequestFailed(Exception):
    """A non-404 upstream failure: the whole batch must fail."""


@dataclass(slots=True)
class Waiter:
    """One batch's interest in a shared query."""

    event: asyncio.Event = field(default_factory=asyncio.Event)
    value: dict[str, str] | None = None
    not_found: bool = False
    error: BaseException | None = None
    released: bool = False

    def resolve(self, value: dict[str, str]) -> None:
        self.value = value
        self.event.set()

    def mark_not_found(self) -> None:
        self.not_found = True
        self.event.set()

    def fail(self, exc: BaseException) -> None:
        self.error = exc
        self.event.set()

    def release(self) -> None:
        """Wake a waiter whose batch no longer cares about the answer."""
        self.released = True
        self.event.set()


class Query:
    """A single in-flight upstream lookup shared by any number of batches."""

    __slots__ = ("gene_id", "waiters", "task", "done")

    def __init__(self, gene_id: str) -> None:
        self.gene_id = gene_id
        self.task: asyncio.Task[None] | None = None
        self.waiters: list[Waiter] = []
        self.done = False


class AnnotationHub:
    """Single-flight registry in front of the annotation source."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client
        self._queries: dict[str, Query] = {}
        self._lock = asyncio.Lock()

    @property
    def in_flight(self) -> int:
        return len(self._queries)

    def in_flight_genes(self) -> list[str]:
        return sorted(self._queries)

    async def lookup(
        self, gene_id: str, cancelled: asyncio.Event
    ) -> dict[str, str] | None:
        """Resolve ``gene_id``; return ``None`` for a 404.

        Raises :class:`BatchCancelled` as soon as ``cancelled`` is set and
        :class:`AnnotationRequestFailed` when the upstream fails with anything
        other than 404.  A batch's cancellation never cancels a query other
        batches are still waiting on.
        """
        async with self._lock:
            if cancelled.is_set():
                raise BatchCancelled()
            query = self._queries.get(gene_id)
            if query is None:
                query = Query(gene_id)
                self._queries[gene_id] = query
                query.task = asyncio.create_task(self._drive(query))
                log.debug("query %s started", gene_id)
            else:
                log.debug(
                    "query %s shared with %d waiter(s)",
                    gene_id,
                    len(query.waiters),
                )
            waiter = Waiter()
            query.waiters.append(waiter)
            wake = asyncio.create_task(self._release_on_cancel(waiter, cancelled))

        try:
            await waiter.event.wait()
        except asyncio.CancelledError:
            # The batch task was cancelled while waiting: release its interest
            # immediately so the query can be aborted if nobody else wants it.
            self._detach(query, waiter)
            raise
        finally:
            wake.cancel()

        if cancelled.is_set() or waiter.released:
            # This batch went away: give up its interest, and abort the lookup
            # only if nobody else is waiting on it.
            self._detach(query, waiter)
            raise BatchCancelled()

        if waiter.value is not None:
            return waiter.value
        if waiter.not_found:
            return None
        if waiter.error is not None:
            raise waiter.error
        raise BatchCancelled()

    @staticmethod
    async def _release_on_cancel(waiter: Waiter, cancelled: asyncio.Event) -> None:
        await cancelled.wait()
        waiter.release()

    def _detach(self, query: Query, waiter: Waiter) -> None:
        if waiter in query.waiters:
            query.waiters.remove(waiter)
        if query.waiters or query.done:
            if query.waiters:
                log.debug(
                    "query %s kept for %d remaining waiter(s)",
                    query.gene_id,
                    len(query.waiters),
                )
            return
        # Last interested batch is gone before an answer arrived: abort.
        if self._queries.get(query.gene_id) is query:
            del self._queries[query.gene_id]
        if query.task is not None and not query.task.done():
            query.task.cancel()
        log.debug("query %s aborted (no waiters left)", query.gene_id)

    async def _drive(self, query: Query) -> None:
        """Run one HTTP lookup and fan its outcome out to whoever is waiting."""
        url = f"{self._client.base_url}/annotations/{query.gene_id}"
        try:
            response = await self._client.get(url)
            if response.status_code == 404:
                outcome: dict[str, str] | None = None
            elif response.status_code == 200:
                outcome = response.json()
            else:
                raise AnnotationRequestFailed(
                    f"annotation source returned HTTP {response.status_code}"
                )
        except asyncio.CancelledError:
            # Aborted because every interested batch went away. No result.
            log.debug("query %s cancelled in flight", query.gene_id)
            raise
        except AnnotationRequestFailed as exc:
            self._settle(query, error=exc)
            return
        except Exception as exc:  # transport errors, malformed JSON, ...
            self._settle(
                query,
                error=AnnotationRequestFailed(
                    f"annotation source request failed: {exc!r}"
                ),
            )
            return
        self._settle(query, value=outcome)

    def _settle(
        self,
        query: Query,
        *,
        value: dict[str, str] | None = None,
        error: BaseException | None = None,
    ) -> None:
        """Deliver the outcome, retire the entry, and never cache it."""
        if query.done:
            return
        query.done = True
        # Retire by identity so a newer query for the same gene is untouched.
        if self._queries.get(query.gene_id) is query:
            del self._queries[query.gene_id]
        waiters, query.waiters = query.waiters, []
        for waiter in waiters:
            if value is not None:
                waiter.resolve(value)
            elif error is not None:
                waiter.fail(error)
            else:
                waiter.mark_not_found()

    async def close(self) -> None:
        for query in list(self._queries.values()):
            if query.task is not None and not query.task.done():
                query.task.cancel()
        self._queries.clear()
        # Let the cancelled HTTP tasks unwind before the client is closed.
        await asyncio.sleep(0)
