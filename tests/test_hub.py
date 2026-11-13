"""Shared single-flight lookups and their cancellation semantics."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from app.hub import AnnotationHub, BatchCancelled
from tests.source_harness import SourceHarness
from tests.test_batches import start, wait_for_state


async def test_one_batch_cancel_leaves_shared_query_alive(api, source):
    """Cancelling a batch must not cancel a query another batch still wants."""
    source.delay = 0.5
    shared = "ENSG00000141510"
    first = await start(api, [shared + ".18"])
    second = await start(api, [shared + ".3"])
    await asyncio.sleep(0.1)
    assert source.counts[shared] == 1

    await api.post(f"/batches/{first['batch_id']}/cancel")
    # The query is not aborted while the second batch still waits on it.
    await asyncio.sleep(0.05)
    assert api.app.state.hub.in_flight_genes() == [shared]
    assert source.counts[shared] == 1

    state = await wait_for_state(api, second["batch_id"])
    assert state["state"] == "completed"
    assert source.counts[shared] == 1


async def test_last_waiter_cancel_removes_query_immediately(api, source):
    source.delay = 0.5
    created = await start(api, ["ENSG00000141510.18"])
    await asyncio.sleep(0.1)
    await api.post(f"/batches/{created['batch_id']}/cancel")
    await asyncio.sleep(0.05)
    assert api.app.state.hub.in_flight_genes() == []


async def test_cancelled_before_lookup_starts_sends_no_request(api, source):
    """A batch cancelled before any await issues no upstream traffic."""
    source.delay = 0.5
    created = await start(api, ["ENSG00000141510.1", "ENSG00000012048.2"])
    # The request is sent from a background task; cancel at once.
    await api.post(f"/batches/{created['batch_id']}/cancel")
    state = await wait_for_state(api, created["batch_id"])
    assert state["state"] == "cancelled"
    await asyncio.sleep(0.1)
    assert source.requests == [], source.requests


async def test_hub_lookup_cancel_returns_batch_cancelled(source):
    source.delay = 0.4
    hub = AnnotationHub(source.client())
    cancelled = asyncio.Event()

    async def resolve() -> dict | None:
        return await hub.lookup("ENSG00000141510", cancelled)

    task = asyncio.create_task(resolve())
    await asyncio.sleep(0.05)
    cancelled.set()
    with pytest.raises(BatchCancelled):
        await task
    await asyncio.sleep(0.05)
    assert hub.in_flight_genes() == []
    await hub.close()


async def test_hub_reports_non_404_as_failure(source):
    from app.hub import AnnotationRequestFailed

    async def teapot(request: httpx.Request) -> httpx.Response | None:
        return httpx.Response(418, json={"detail": "teapot"})

    source.on_request(teapot)
    hub = AnnotationHub(source.client())
    cancelled = asyncio.Event()
    with pytest.raises(AnnotationRequestFailed, match="418"):
        await hub.lookup("ENSG00000141510", cancelled)
    await hub.close()


async def test_hub_404_is_not_an_error(source):
    hub = AnnotationHub(source.client())
    cancelled = asyncio.Event()
    assert await hub.lookup("ENSG00000000001", cancelled) is None
    await hub.close()
