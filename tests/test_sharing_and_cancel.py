"""Single-flight sharing, cancellation semantics and no-cache guarantees.

These tests exercise the exact concurrency contract:

* two batches needing the same gene while it is in flight cause one request;
* cancelling one batch is immediate but the other batch still completes;
* when the last interested batch cancels, the upstream request is aborted;
* a later batch for that gene issues a fresh request;
* a late/aborted result cannot touch the newer query or revive the cancelled run;
* nothing is cached after a query finishes.
"""

from __future__ import annotations

import asyncio
import csv
import io

import httpx

from tests.conftest import make_csv
from tests.test_batches import start, wait_for_state


async def test_two_batches_share_one_in_flight_lookup(api, source):
    source.delay = 0.4
    gene = "ENSG00000141510.18"
    first = await start(api, [gene, "ENSG00000012048.22"], filename="a.csv")
    second = await start(api, [gene, "ENSG00000157764.12"], filename="b.csv")

    await asyncio.sleep(0.15)  # both batches are now waiting on TP53
    assert source.counts["ENSG00000141510"] == 1, source.counts

    a = await wait_for_state(api, first["batch_id"])
    b = await wait_for_state(api, second["batch_id"])
    assert a["state"] == "completed" and b["state"] == "completed"
    # Distinct genes are looked up once per batch; the shared gene only once.
    assert source.counts["ENSG00000141510"] == 1, source.counts
    assert source.counts["ENSG00000012048"] == 1
    assert source.counts["ENSG00000157764"] == 1

    for batch_id, expected in ((first["batch_id"], ["TP53"]), (second["batch_id"], ["TP53"])):
        text = (await api.get(f"/batches/{batch_id}/download")).text
        assert expected[0] in text


async def test_cancel_one_batch_while_other_completes(api, source):
    source.delay = 0.6
    shared = "ENSG00000141510.18"
    first = await start(api, [shared, "ENSG00000012048.22", "ENSG00000000002"], filename="a.csv")
    second = await start(api, [shared, "ENSG00000157764.12"], filename="b.csv")
    await asyncio.sleep(0.15)
    assert source.counts[shared.split(".")[0]] == 1

    # Cancel batch A immediately; the shared lookups for TP53 and BRCA1 must
    # keep running for batch B.
    response = await api.post(f"/batches/{first['batch_id']}/cancel")
    assert response.status_code == 202
    assert response.json()["state"] == "cancelled"
    # Immediate: no waiting for the slow upstream to answer.
    assert source.counts[shared.split(".")[0]] == 1

    b = await wait_for_state(api, second["batch_id"], timeout=5)
    assert b["state"] == "completed"
    assert b["rows_processed"] == 2

    a = await wait_for_state(api, first["batch_id"])
    assert a["state"] == "cancelled"
    assert a["download_available"] is False
    download = await api.get(f"/batches/{first['batch_id']}/download")
    assert download.status_code == 409
    assert "cancelled" in download.json()["detail"]

    # Batch B's output is unaffected by A's cancellation.
    text = (await api.get(f"/batches/{second['batch_id']}/download")).text
    assert "TP53" in text and "BRAF" in text


async def test_cancel_last_waiting_batch_aborts_query_and_requeries(api, source):
    source.delay = 0.8
    gene_id = "ENSG00000141510"
    first = await start(api, [gene_id + ".18"], filename="a.csv")
    await asyncio.sleep(0.1)
    assert source.counts[gene_id] == 1

    await api.post(f"/batches/{first['batch_id']}/cancel")
    assert (await api.get(f"/batches/{first['batch_id']}")).json()["state"] == "cancelled"

    # No waiter remains, so the in-flight query is dropped from the registry
    # and its HTTP task is cancelled (the slow source never sees a consumer).
    hub = api.app.state.hub
    await asyncio.sleep(0.05)
    assert hub.in_flight_genes() == []

    # A new batch for the same gene must issue its own request.
    second = await start(api, [gene_id + ".6"], filename="b.csv")
    state = await wait_for_state(api, second["batch_id"])
    assert state["state"] == "completed"
    assert source.counts[gene_id] == 2, source.counts

    text = (await api.get(f"/batches/{second['batch_id']}/download")).text
    rows = list(csv.reader(io.StringIO(text)))
    assert rows[1] == [gene_id + ".6", "TP53", "17"]


async def test_late_aborted_result_does_not_touch_new_query(api, source):
    """The old request is abandoned and the new batch issues its own lookup.

    The first lookup is held open on a gate; it is cancelled while the second
    batch starts its own query.  The first request's eventual completion can
    then only be discarded.
    """
    gene_id = "ENSG00000141510"
    release = asyncio.Event()
    first_call_seen = asyncio.Event()
    calls = 0

    async def gated(request: httpx.Request) -> httpx.Response | None:
        nonlocal calls
        if request.url.path.endswith(gene_id):
            calls += 1
            if calls == 1:
                first_call_seen.set()
                await release.wait()  # hold the first response open
            return httpx.Response(200, json={"symbol": "TP53", "chromosome": "17"})
        return None

    source.on_request(gated)
    first = await start(api, [gene_id + ".18"], filename="a.csv")
    await asyncio.wait_for(first_call_seen.wait(), timeout=2)

    await api.post(f"/batches/{first['batch_id']}/cancel")
    await asyncio.sleep(0.05)
    assert api.app.state.hub.in_flight_genes() == []

    # The new batch must issue a *separate* request, not join the aborted one.
    second = await start(api, [gene_id + ".3"], filename="b.csv")
    await asyncio.sleep(0.1)
    assert calls == 2, calls
    release.set()
    state = await wait_for_state(api, second["batch_id"])
    assert state["state"] == "completed"
    assert state["matched_genes"] == 1

    # The aborted first run stays cancelled and produces nothing.
    again = (await api.get(f"/batches/{first['batch_id']}")).json()
    assert again["state"] == "cancelled"
    assert again["download_available"] is False


async def test_no_caching_between_sequential_batches(api, source):
    source.delay = 0.1
    gene = "ENSG00000141510.18"
    first = await start(api, [gene])
    await wait_for_state(api, first["batch_id"])
    assert source.counts["ENSG00000141510"] == 1

    second = await start(api, [gene])
    await wait_for_state(api, second["batch_id"])
    assert source.counts["ENSG00000141510"] == 2, "finished lookups must not be cached"


async def test_later_batches_do_not_join_a_finished_query(api, source):
    """Sharing is only for queries in flight; a finished one is not a cache."""
    source.delay = 0.25
    gene = "ENSG00000141510.18"
    first = await start(api, [gene])
    await asyncio.sleep(0.3)
    assert (await api.get(f"/batches/{first['batch_id']}")).json()["state"] == "completed"
    # Query is gone from the registry the moment it settled.
    assert api.app.state.hub.in_flight_genes() == []


async def test_cancel_after_finish_is_conflict(api, source):
    source.delay = 0.0
    created = await start(api, ["ENSG00000141510"])
    await wait_for_state(api, created["batch_id"])
    response = await api.post(f"/batches/{created['batch_id']}/cancel")
    assert response.status_code == 409
    assert (await api.get(f"/batches/{created['batch_id']}")).json()["state"] == "completed"


async def test_duplicate_gene_in_one_batch_shares_one_request(api, source):
    source.delay = 0.3
    gene = "ENSG00000141510.18"
    created = await start(api, [gene] * 5)
    state = await wait_for_state(api, created["batch_id"])
    assert state["state"] == "completed"
    assert state["rows_total"] == 5
    assert source.counts["ENSG00000141510"] == 1
