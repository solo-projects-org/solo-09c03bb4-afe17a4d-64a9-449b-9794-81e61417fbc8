"""The local annotation source as an in-process ASGI app for tests.

Tests point the merge service's ``httpx.AsyncClient`` at ``ASGITransport`` so
the real FastAPI app runs without opening a socket, and each test can wrap its
routing to count requests, add latency or inject failures.
"""

from __future__ import annotations

import asyncio
import collections
from typing import Awaitable, Callable

import httpx

from annotation_source.main import create_app as create_source_app
from annotation_source.service import load_reference

RequestHook = Callable[[httpx.Request], Awaitable[httpx.Response | None]]


class SourceHarness:
    """Runs the annotation source in-process and records every request."""

    def __init__(self, *, delay: float = 0.0) -> None:
        self.app = create_source_app()
        self.app.state.reference = load_reference("/nonexistent", "unused.tsv")
        self.app.state.requests_total = 0
        self.app.state.requests_per_gene = {}
        self.delay = delay
        self.requests: list[str] = []
        self._hook: RequestHook | None = None

    @property
    def counts(self) -> collections.Counter[str]:
        return collections.Counter(self.requests)

    def on_request(self, hook: RequestHook | None) -> None:
        self._hook = hook

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append(path.rsplit("/", 1)[-1] if "/annotations/" in path else path)
        if self._hook is not None:
            override = await self._hook(request)
            if override is not None:
                return override
        if self.delay:
            await asyncio.sleep(self.delay)
        row = None
        if path.startswith("/annotations/"):
            row = self.app.state.reference.get(path.rsplit("/", 1)[-1])
        if row is None:
            return httpx.Response(404, json={"detail": "unknown"})
        return httpx.Response(200, json=row)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="http://annotation-source", transport=httpx.MockTransport(self.handle)
        )
