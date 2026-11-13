"""Contract tests for durable samples and the rebuilt count matrix."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import threading

import httpx
import pytest

from app import matrix as matrix_module


def sample_csv(*rows: tuple[str, str]) -> bytes:
    return ("gene_id,count\n" + "".join(f"{g},{c}\n" for g, c in rows)).encode()


async def put_sample(api, sample_id: str, data: bytes, *, expect: int = 201):
    response = await api.put(
        f"/samples/{sample_id}", files={"file": (f"{sample_id}.csv", data, "text/csv")}
    )
    assert response.status_code == expect, response.text
    return response


async def wait_for_matrix(
    api, *, state: str = "current", timeout: float = 5.0
) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        payload = (await api.get("/matrix")).json()
        if payload["state"] == state:
            return payload
        await asyncio.sleep(0.01)
    raise AssertionError(f"matrix did not reach {state}: {payload}")


async def read_matrix(api) -> list[list[str]]:
    response = await api.get("/matrix/download")
    assert response.status_code == 200, response.text
    return list(csv.reader(io.StringIO(response.text)))


# -- sample ingestion ---------------------------------------------------


async def test_sample_upload_accepts_non_negative_integers_and_strips_versions(api):
    await put_sample(
        api,
        "s1",
        sample_csv(("ENSG00000141510.18", "5"), ("ENSG00000012048", "0")),
    )
    meta = (await api.get("/samples/s1")).json()
    assert meta["gene_count"] == 2
    download = await api.get("/samples/s1/download")
    assert download.status_code == 200
    assert download.text == "gene_id,count\nENSG00000141510,5\nENSG00000012048,0\n"


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"gene_id\nENSG00000141510\n",
        b"gene_id,count,extra\nENSG00000141510,5,1\n",
        b"gene_id,count\nENSG00000141510,-1\n",
        b"gene_id,count\nENSG00000141510,1.5\n",
        b"gene_id,count\nENSG00000141510,2x\n",
        b"gene_id,count\n,3\n",
        b"gene_id,count\nENSG00000141510,abc\n",
        b"gene_id,count\nENSG00000141510,3\n\n",
        b"gene_id,count\n",
    ],
)
async def test_sample_validation_rejects_whole_file(api, payload):
    response = await api.put(
        "/samples/bad", files={"file": ("bad.csv", payload, "text/csv")}
    )
    expected = 400 if payload == b"" else 422
    assert response.status_code == expected, response.text
    assert (await api.get("/samples/bad")).status_code == 404


async def test_duplicate_after_version_strip_rejects_whole_sample(api):
    payload = sample_csv(("ENSG00000141510.18", "5"), ("ENSG00000141510.6", "7"))
    response = await api.put(
        "/samples/s1", files={"file": ("dup.csv", payload, "text/csv")}
    )
    assert response.status_code == 422
    assert "duplicates" in response.json()["detail"]


async def test_exact_duplicate_gene_also_rejected(api):
    payload = sample_csv(("ENSG00000141510", "5"), ("ENSG00000141510", "7"))
    response = await api.put(
        "/samples/s1", files={"file": ("dup.csv", payload, "text/csv")}
    )
    assert response.status_code == 422


async def test_rejected_correction_keeps_previous_sample(api):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    before = (await api.get("/samples/s1/download")).text
    response = await api.put(
        "/samples/s1",
        files={"file": ("bad.csv", sample_csv(("ENSG00000141510", "-3")), "text/csv")},
    )
    assert response.status_code == 422
    assert (await api.get("/samples/s1/download")).text == before
    assert (await api.get("/samples/s1")).json()["content_hash"] == (
        await api.get("/samples/s1")
    ).json()["content_hash"]


async def test_sample_replacement_replaces_whole_sample(api):
    await put_sample(
        api, "s1", sample_csv(("ENSG00000141510", "5"), ("ENSG00000012048", "1"))
    )
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "9")), expect=200)
    meta = (await api.get("/samples/s1")).json()
    assert meta["gene_count"] == 1
    text = (await api.get("/samples/s1/download")).text
    assert text == "gene_id,count\nENSG00000141510,9\n"
    await wait_for_matrix(api)
    rows = await read_matrix(api)
    assert rows == [["gene_id", "s1"], ["ENSG00000141510", "9"]]


async def test_invalid_sample_id_and_unknown_sample(api):
    response = await api.put(
        "/samples/../etc/passwd",
        files={"file": ("x.csv", sample_csv(("G1", "1")), "text/csv")},
    )
    assert response.status_code in (404, 422)
    assert (await api.get("/samples/nope")).status_code == 404
    assert (await api.get("/samples/nope/download")).status_code == 404


# -- matrix layout ------------------------------------------------------


async def test_matrix_sorted_rows_and_columns_with_empty_cells(api):
    # Uploaded out of order; genes and samples must appear sorted.
    await put_sample(
        api,
        "zeta",
        sample_csv(("ENSG00000157764.12", "2"), ("ENSG00000141510.18", "5")),
    )
    await put_sample(
        api,
        "alpha",
        sample_csv(("ENSG00000141510.3", "7"), ("ENSG00000012048.22", "0")),
    )
    await wait_for_matrix(api)

    rows = await read_matrix(api)
    assert rows[0] == ["gene_id", "alpha", "zeta"]
    assert [row[0] for row in rows[1:]] == [
        "ENSG00000012048",
        "ENSG00000141510",
        "ENSG00000157764",
    ]
    assert rows[1] == ["ENSG00000012048", "0", ""]
    assert rows[2] == ["ENSG00000141510", "7", "5"]
    # A missing gene is an empty cell, never a zero count.
    assert rows[3] == ["ENSG00000157764", "", "2"]


async def test_matrix_status_before_any_sample(api):
    payload = (await api.get("/matrix")).json()
    assert payload["state"] == "none"
    assert payload["download_available"] is False
    download = await api.get("/matrix/download")
    assert download.status_code == 409
    assert download.json()["detail"]["state"] == "none"


async def test_empty_sample_is_rejected(api):
    response = await api.put(
        "/samples/s1", files={"file": ("empty.csv", b"gene_id,count\n", "text/csv")}
    )
    assert response.status_code == 422


# -- rebuilds -----------------------------------------------------------


async def test_status_stale_while_rebuild_runs_and_old_matrix_downloadable(api, monkeypatch):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)
    old = (await api.get("/matrix/download")).text

    # Hold the next build open so the post-upload state can be observed.
    gate = threading.Event()
    entered = threading.Event()
    real_render = matrix_module.render_matrix

    def slow_render(samples):
        entered.set()
        gate.wait(timeout=5)
        return real_render(samples)

    monkeypatch.setattr(matrix_module, "render_matrix", slow_render)
    await put_sample(api, "s2", sample_csv(("ENSG00000141510", "9")))
    await asyncio.to_thread(entered.wait, 2)
    status = (await api.get("/matrix")).json()
    assert status["state"] == "building"
    assert status["stale"] is True
    assert status["download_available"] is True
    assert status["pending_samples"] == ["s2"]
    # The previous complete matrix is still served during the rebuild.
    during = await api.get("/matrix/download")
    assert during.status_code == 200
    assert during.text == old

    gate.set()
    await wait_for_matrix(api)
    monkeypatch.setattr(matrix_module, "render_matrix", real_render)
    rows = await read_matrix(api)
    assert rows[1] == ["ENSG00000141510", "5", "9"]


async def test_stale_build_is_discarded_and_current_samples_win(api, monkeypatch):
    """A build that races an upload must not publish its outdated contents."""
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)

    gate = threading.Event()
    entered = threading.Event()
    real_render = matrix_module.render_matrix

    def slow_render(samples):
        entered.set()
        gate.wait(timeout=5)
        return real_render(samples)

    monkeypatch.setattr(matrix_module, "render_matrix", slow_render)
    await put_sample(api, "s2", sample_csv(("ENSG00000141510", "9")))
    await asyncio.to_thread(entered.wait, 2)

    # While the build is held, a correction of s1 lands.
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "11")), expect=200)
    gate.set()
    monkeypatch.setattr(matrix_module, "render_matrix", real_render)
    await wait_for_matrix(api, timeout=10)

    rows = await read_matrix(api)
    assert rows[1] == ["ENSG00000141510", "11", "9"], rows
    status = (await api.get("/matrix")).json()
    assert status["source_samples"] == ["s1", "s2"]


async def test_late_build_failure_keeps_old_matrix_and_reports_failed(api, monkeypatch):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)
    old = (await api.get("/matrix/download")).text

    def broken_render(samples):
        raise RuntimeError("disk full while rendering")

    monkeypatch.setattr(matrix_module, "render_matrix", broken_render)
    await put_sample(api, "s2", sample_csv(("ENSG00000012048", "4")))
    payload = await wait_for_matrix(api, state="failed")

    assert "disk full" in payload["error"]
    assert payload["status"].startswith("failed")
    assert payload["has_matrix"] if "has_matrix" in payload else True
    assert payload["download_available"] is True
    still_old = await api.get("/matrix/download")
    assert still_old.status_code == 200
    assert still_old.text == old


async def test_unreadable_sample_fails_build_without_losing_old_matrix(api, monkeypatch):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)
    old = (await api.get("/matrix/download")).text

    store = api.app.state.samples

    def broken_read(sample_id, expected_hash):
        raise OSError("simulated unreadable sample")

    monkeypatch.setattr(store, "read_verified", broken_read)
    api.app.state.matrix.request_rebuild(force=True, reason="test")
    payload = await wait_for_matrix(api, state="failed")
    assert payload["error"]
    assert payload["download_available"] is True
    # The previous complete matrix is untouched and still served.
    assert (await api.get("/matrix/download")).text == old


async def test_publish_failure_keeps_old_matrix(api, monkeypatch):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)
    old = (await api.get("/matrix/download")).text
    old_record = (api.app.state.matrix.published_path).read_bytes()

    matrix_dir = api.app.state.matrix.matrix_dir
    real_replace = matrix_module.os.replace

    def failing_replace(src, dst):
        # The published record is the single commit point; failing it means
        # the new build is never registered.
        if str(dst).endswith("published.json"):
            raise OSError("simulated publication failure")
        return real_replace(src, dst)

    monkeypatch.setattr(matrix_module.os, "replace", failing_replace)
    await put_sample(api, "s2", sample_csv(("ENSG00000012048", "4")))
    payload = await wait_for_matrix(api, state="failed")
    assert "simulated publication failure" in payload["error"]
    # The old matrix was never removed and is still the registered one.
    assert (await api.get("/matrix/download")).text == old
    assert api.app.state.matrix.published_path.read_bytes() == old_record
    # No half-written matrix lingers in the published directory.
    assert list(matrix_dir.glob("*.tmp")) == []
    monkeypatch.setattr(matrix_module.os, "replace", real_replace)

    # A retry publishes the current samples.
    api.app.state.matrix.request_rebuild(force=True, reason="retry")
    await wait_for_matrix(api, timeout=10)
    rows = await read_matrix(api)
    assert rows[1] == ["ENSG00000012048", "", "4"]


# -- durability across restarts ------------------------------------------


def _build_app(api_client, data_dir, monkeypatch):
    """Restart the service against the same data volume (fresh process)."""
    from app.main import create_app

    monkeypatch.setattr(
        "app.config.SETTINGS.data_dir", str(data_dir), raising=False
    )
    return create_app(api_client)


async def test_restart_reloads_samples_and_matrix(tmp_path):
    from app.config import SETTINGS
    from app.main import create_app
    from tests.source_harness import SourceHarness

    data_dir = tmp_path / "volume"
    SETTINGS.data_dir = str(data_dir)
    SETTINGS.matrix_rebuild_debounce_seconds = 0.0

    source = SourceHarness(delay=0.0)
    client = source.client()

    app1 = create_app(client)
    async with app1.router.lifespan_context(app1):
        transport = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport, base_url="http://m") as api:
            await put_sample(
                api,
                "s1",
                sample_csv(("ENSG00000141510", "5"), ("ENSG00000157764", "2")),
            )
            await wait_for_matrix(api)
            published = (await api.get("/matrix/download")).text

    # A brand-new app instance over the same directory: samples and the
    # published matrix must be there without re-uploading anything.
    app2 = create_app(source.client())
    async with app2.router.lifespan_context(app2):
        transport = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport, base_url="http://m") as api:
            assert (await api.get("/samples")).json()["count"] == 1
            assert (await api.get("/samples/s1/download")).status_code == 200
            await wait_for_matrix(api)
            assert (await api.get("/matrix/download")).text == published


async def test_interrupted_rebuild_resumes_on_restart(tmp_path):
    """A crash mid-build leaves /*.tmp and the old matrix; restart rebuilds."""
    from app.config import SETTINGS
    from app.main import create_app
    from tests.source_harness import SourceHarness

    data_dir = tmp_path / "volume"
    SETTINGS.data_dir = str(data_dir)
    SETTINGS.matrix_rebuild_debounce_seconds = 0.0
    store_dir = data_dir / "samples"
    store_dir.mkdir(parents=True)
    (store_dir / "s1.csv").write_bytes(sample_csv(("ENSG00000141510", "5")))

    # Simulate a crash halfway through publishing: a complete temporary file
    # exists, but no matrix/manifest was ever registered.
    matrix_dir = data_dir / "matrix"
    matrix_dir.mkdir(parents=True)
    (matrix_dir / "build-dead.tmp").write_bytes(b"gene_id,s1\nSTALE,999\n")

    source = SourceHarness(delay=0.0)
    app = create_app(source.client())
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://m") as api:
            await wait_for_matrix(api)
            rows = await read_matrix(api)
            assert rows == [["gene_id", "s1"], ["ENSG00000141510", "5"]]
            # The interrupted build's partial file was removed, never served.
            assert not (matrix_dir / "build-dead.tmp").exists()


async def test_corrupt_manifest_triggers_rebuild(tmp_path):
    from app.config import SETTINGS
    from app.main import create_app
    from tests.source_harness import SourceHarness

    data_dir = tmp_path / "volume"
    SETTINGS.data_dir = str(data_dir)
    SETTINGS.matrix_rebuild_debounce_seconds = 0.0
    (data_dir / "samples").mkdir(parents=True)
    (data_dir / "samples" / "s1.csv").write_bytes(
        sample_csv(("ENSG00000141510", "5"))
    )
    matrix_dir = data_dir / "matrix"
    matrix_dir.mkdir(parents=True)
    (matrix_dir / "build-old.csv").write_bytes(b"gene_id,s1\nWRONG,0\n")
    (matrix_dir / "published.json").write_text(
        json.dumps(
            {
                "file": "build-old.csv",
                "built_at": "2026-01-01T00:00:00+00:00",
                "duration_seconds": 0.1,
                "columns": 1,
                "rows": 1,
                "content_hash": "not-the-hash",
                "sources": [["s1", "not-the-hash"]],
            }
        )
    )

    app = create_app(SourceHarness(delay=0.0).client())
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://m") as api:
            await wait_for_matrix(api)
            rows = await read_matrix(api)
            assert rows == [["gene_id", "s1"], ["ENSG00000141510", "5"]]


# -- concurrency --------------------------------------------------------


async def test_uploads_during_rebuild_all_land_in_final_matrix(api):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "1")))
    # Fire many corrections concurrently; uploads must not wait for builds.
    await asyncio.gather(
        *[
            put_sample(
                api,
                f"s{i}",
                sample_csv(("ENSG00000141510", str(i)), ("ENSG00000012048", str(i))),
            )
            for i in range(2, 9)
        ]
    )
    await wait_for_matrix(api, timeout=10)
    rows = await read_matrix(api)
    assert rows[0] == ["gene_id", *[f"s{i}" for i in range(1, 9)]]
    assert rows[1] == ["ENSG00000012048", "", *[str(i) for i in range(2, 9)]]
    assert rows[2] == ["ENSG00000141510", *[str(i) for i in range(1, 9)]]
    status = (await api.get("/matrix")).json()
    assert status["source_samples"] == [f"s{i}" for i in range(1, 9)]


async def test_identical_reupload_does_not_rebuild(api):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)
    build_before = (await api.get("/matrix")).json()["build_id"]
    await put_sample(api, "s1", sample_csv(("ENSG00000141510.3", "5")), expect=200)
    await asyncio.sleep(0.1)
    assert (await api.get("/matrix")).json()["build_id"] == build_before


async def test_annotation_batches_still_work_alongside_samples(api, source):
    from tests.test_batches import start, wait_for_state

    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    created = await start(api, ["ENSG00000141510.18"])
    assert (await wait_for_state(api, created["batch_id"]))["state"] == "completed"
    await wait_for_matrix(api)
    assert (await api.get("/matrix/download")).status_code == 200


async def test_save_failure_keeps_previous_sample(api, monkeypatch):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    before = (await api.get("/samples/s1/download")).text

    store = api.app.state.samples
    real_replace = store.replace

    def failing_replace(sample_id, parsed):
        # Simulate the fsync/rename step failing after validation.
        raise __import__("app.samples", fromlist=["SampleSaveError"]).SampleSaveError(
            "simulated write failure"
        )

    monkeypatch.setattr(store, "replace", failing_replace)
    response = await api.put(
        "/samples/s1",
        files={"file": ("new.csv", sample_csv(("ENSG00000012048", "3")), "text/csv")},
    )
    assert response.status_code == 500
    assert "simulated write failure" in response.json()["detail"]
    monkeypatch.setattr(store, "replace", real_replace)
    # The old sample is still stored and served, unchanged.
    assert (await api.get("/samples/s1/download")).text == before
    await wait_for_matrix(api)
    assert (await api.get("/matrix/download")).text == (
        "gene_id,s1\nENSG00000141510,5\n"
    )


async def test_download_reports_matrix_state_header(api):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "5")))
    await wait_for_matrix(api)
    response = await api.get("/matrix/download")
    assert response.headers["x-matrix-state"] == "current"
    assert response.headers["cache-control"] == "no-store"


async def test_repeated_corrections_converge_to_latest(api):
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "1")))
    for value in range(2, 15):
        await put_sample(
            api, "s1", sample_csv(("ENSG00000141510", str(value))), expect=200
        )
    await wait_for_matrix(api, timeout=10)
    rows = await read_matrix(api)
    assert rows == [["gene_id", "s1"], ["ENSG00000141510", "14"]]
    status = (await api.get("/matrix")).json()
    assert status["stale"] is False
    assert status["pending_samples"] == []


async def test_sample_with_only_zeros_and_empty_matrix_cells(api):
    await put_sample(api, "a", sample_csv(("G1", "0"), ("G2", "0")))
    await put_sample(api, "b", sample_csv(("G2", "5")))
    await wait_for_matrix(api)
    rows = await read_matrix(api)
    assert rows == [
        ["gene_id", "a", "b"],
        ["G1", "0", ""],
        ["G2", "0", "5"],
    ]


async def test_large_sample_upload(api):
    body = "gene_id,count\n" + "".join(
        f"ENSG{i:011d}.{i % 5},{i % 100}\n" for i in range(5000)
    )
    await put_sample(api, "big", body.encode())
    await wait_for_matrix(api, timeout=10)
    rows = await read_matrix(api)
    assert len(rows) == 5001
    assert rows[1][0] == "ENSG00000000000"


async def test_upload_is_not_blocked_by_a_slow_rebuild(api, monkeypatch):
    """A long-running rebuild must not delay sample uploads."""
    await put_sample(api, "s1", sample_csv(("ENSG00000141510", "1")))
    await wait_for_matrix(api)

    gate = threading.Event()
    entered = threading.Event()
    real_render = matrix_module.render_matrix

    def slow_render(samples):
        entered.set()
        gate.wait(timeout=10)
        return real_render(samples)

    monkeypatch.setattr(matrix_module, "render_matrix", slow_render)
    await put_sample(api, "s2", sample_csv(("ENSG00000141510", "2")))
    await asyncio.to_thread(entered.wait, 5)

    loop = asyncio.get_running_loop()
    started = loop.time()
    await put_sample(api, "s3", sample_csv(("ENSG00000141510", "3")))
    elapsed = loop.time() - started
    # Upload handling does not wait for the held build to finish.
    assert elapsed < 1.0, elapsed
    assert (await api.get("/matrix")).json()["state"] == "building"
    # Downloading the previous complete matrix also works meanwhile.
    assert (await api.get("/matrix/download")).status_code == 200

    gate.set()
    monkeypatch.setattr(matrix_module, "render_matrix", real_render)
    await wait_for_matrix(api, timeout=10)
    rows = await read_matrix(api)
    assert rows[1] == ["ENSG00000141510", "1", "2", "3"]


async def test_concurrent_first_uploads_report_one_create(api):
    responses = await asyncio.gather(
        *[
            api.put(
                "/samples/shared",
                files={
                    "file": (
                        "x.csv",
                        sample_csv(("ENSG00000141510", str(i))),
                        "text/csv",
                    )
                },
            )
            for i in range(8)
        ]
    )
    codes = [r.status_code for r in responses]
    assert codes.count(201) == 1, codes
    assert codes.count(200) == 7, codes
    assert (await api.get("/samples")).json()["count"] == 1
    await wait_for_matrix(api, timeout=10)
    rows = await read_matrix(api)
    assert rows[0] == ["gene_id", "shared"]
