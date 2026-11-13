"""Test fixtures: the merge API and the local source, both in-process."""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio

from app.config import SETTINGS
from app.main import create_app
from tests.source_harness import SourceHarness

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def source() -> SourceHarness:
    return SourceHarness(delay=0.2)


@pytest_asyncio.fixture
async def api(source):
    app = create_app(source.client())
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://merge"
        ) as client:
            client.app = app  # convenience for asserting internal state
            yield client


@pytest.fixture(autouse=True)
def _test_settings(monkeypatch):
    monkeypatch.setattr(SETTINGS, "per_run_concurrency", 8)
    monkeypatch.setattr(SETTINGS, "max_retained_runs", 1000)
    yield


def make_csv(*gene_ids: str) -> bytes:
    return ("gene_id\n" + "\n".join(gene_ids) + "\n").encode()
