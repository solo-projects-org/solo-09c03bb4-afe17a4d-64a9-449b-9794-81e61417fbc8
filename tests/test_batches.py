"""Contract tests for upload, ordering, version suffix, 404 and failures."""

from __future__ import annotations

import asyncio
import csv
import io

import httpx
import pytest

from tests.conftest import make_csv


async def wait_for_state(api, batch_id: str, timeout: float = 5.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        payload = (await api.get(f"/batches/{batch_id}")).json()
        if payload["state"] != "running":
            return payload
        await asyncio.sleep(0.01)
    raise AssertionError(f"batch {batch_id} did not finish: {payload}")


async def start(api, genes: list[str], filename: str = "genes.csv") -> dict:
    response = await api.post(
        "/batches", files={"file": (filename, make_csv(*genes), "text/csv")}
    )
    assert response.status_code == 202, response.text
    return response.json()


async def test_output_preserves_order_duplicates_and_version_suffix(api, source):
    source.delay = 0.0
    genes = [
        "ENSG00000141510.18",
        "ENSG00000012048.22",
        "ENSG00000141510.6",   # duplicate identifier, different version
        "ENSG00000000001",     # unknown to the source -> 404
        "ENSG00000141510.18",  # exact duplicate row
        "ENSG00000157764.12",
    ]
    created = await start(api, genes)
    state = await wait_for_state(api, created["batch_id"])
    assert state["state"] == "completed"
    assert state["rows_total"] == 6
    assert state["rows_processed"] == 6
    assert state["progress_percent"] == 100.0
    assert state["unmatched_genes"] == 1
    assert state["download_available"] is True

    download = await api.get(f"/batches/{created['batch_id']}/download")
    assert download.status_code == 200
    assert download.headers["content-type"].startswith("text/csv")
    rows = list(csv.reader(io.StringIO(download.text)))
    assert rows[0] == ["gene_id", "symbol", "chromosome"]
    assert [row[0] for row in rows[1:]] == genes, "row order and duplicates preserved"
    assert rows[1] == ["ENSG00000141510.18", "TP53", "17"]
    assert rows[2] == ["ENSG00000012048.22", "BRCA1", "17"]
    assert rows[3] == ["ENSG00000141510.6", "TP53", "17"]
    assert rows[4] == ["ENSG00000000001", "unmatched", "unmatched"]
    assert rows[5] == ["ENSG00000141510.18", "TP53", "17"]
    assert rows[6] == ["ENSG00000157764.12", "BRAF", "7"]
    # One shared lookup per distinct normalised identifier, duplicates reused.
    assert sorted(source.requests) == [
        "ENSG00000000001", "ENSG00000012048", "ENSG00000141510", "ENSG00000157764",
    ]


async def test_identifier_without_version_is_kept_as_is(api, source):
    source.delay = 0.0
    created = await start(api, ["ENSG00000244734", "ENSG00000244734.4"])
    state = await wait_for_state(api, created["batch_id"])
    assert state["state"] == "completed"
    download = await api.get(f"/batches/{created['batch_id']}/download")
    rows = list(csv.reader(io.StringIO(download.text)))
    assert rows[1][0] == "ENSG00000244734"
    assert rows[2][0] == "ENSG00000244734.4"


async def test_non_404_upstream_failure_fails_whole_batch(api, source):
    source.delay = 0.0

    async def fail_tp53(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.endswith("ENSG00000141510"):
            return httpx.Response(503, json={"detail": "upstream unavailable"})
        return None

    source.on_request(fail_tp53)
    created = await start(api, ["ENSG00000141510.18", "ENSG00000012048.22"])
    state = await wait_for_state(api, created["batch_id"])
    assert state["state"] == "failed"
    assert "annotation request failed" in state["error"]
    assert state["download_available"] is False

    download = await api.get(f"/batches/{created['batch_id']}/download")
    assert download.status_code == 409
    assert "text/csv" not in download.headers["content-type"]


async def test_transport_error_fails_whole_batch(api, source):
    source.delay = 0.0

    async def boom(request: httpx.Request) -> httpx.Response | None:
        raise httpx.ConnectError("connection refused")

    source.on_request(boom)
    created = await start(api, ["ENSG00000141510"])
    state = await wait_for_state(api, created["batch_id"])
    assert state["state"] == "failed"
    assert state["download_available"] is False


@pytest.mark.parametrize(
    "payload, status",
    [
        (b"", 400),
        (b"gene_id\n", 422),
        (b"symbol,chromosome\nTP53,17\n", 422),
        (b"gene_id,symbol\nTP53,17\n", 422),
        (b"gene_id\n\n", 422),
        (b"gene_id\n \n", 422),
        (b"gene_id\nENSG00000141510\nbroken,row\n", 422),
    ],
)
async def test_upload_validation(api, source, payload, status):
    response = await api.post(
        "/batches", files={"file": ("bad.csv", payload, "text/csv")}
    )
    assert response.status_code == status, response.text


async def test_download_before_completion_is_rejected(api, source):
    source.delay = 0.4
    created = await start(api, ["ENSG00000141510.18", "ENSG00000012048.22"])
    download = await api.get(f"/batches/{created['batch_id']}/download")
    assert download.status_code == 409
    assert "still running" in download.json()["detail"]
    await wait_for_state(api, created["batch_id"])


async def test_unknown_batch(api):
    response = await api.get("/batches/nope")
    assert response.status_code == 404
    response = await api.post("/batches/nope/cancel")
    assert response.status_code == 404
    response = await api.get("/batches/nope/download")
    assert response.status_code == 404
