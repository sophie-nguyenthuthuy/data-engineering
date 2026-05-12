"""Tests for replay targets."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pytest

from replay.models import ArchiveFormat, Event, FileTargetConfig, HttpTargetConfig
from replay.targets.file import FileTarget
from replay.targets.http import HttpTarget
from replay.targets.stdout import StdoutTarget


def _sample_event(offset: int = 1) -> Event:
    return Event(
        topic="orders",
        partition=0,
        offset=offset,
        key=b"order-001",
        value=json.dumps({"order_id": "order-001", "amount": 99.99}).encode(),
        timestamp=datetime(2024, 3, 14, 10, 0, tzinfo=timezone.utc),
        headers={"source": b"replay-test"},
    )


# ─────────────────────────────────────────────
# StdoutTarget
# ─────────────────────────────────────────────

class TestStdoutTarget:
    @pytest.mark.asyncio
    async def test_writes_json_line(self):
        buf = StringIO()
        target = StdoutTarget(stream=buf)
        await target.open()
        await target.send(_sample_event())
        await target.close()

        buf.seek(0)
        line = buf.readline()
        doc = json.loads(line)
        assert doc["topic"] == "orders"
        assert doc["offset"] == 1
        assert doc["value"]["amount"] == 99.99

    @pytest.mark.asyncio
    async def test_pretty_mode(self):
        buf = StringIO()
        target = StdoutTarget(pretty=True, stream=buf)
        await target.open()
        await target.send(_sample_event())
        await target.close()

        output = buf.getvalue()
        assert "\n" in output  # pretty-printed = multi-line


# ─────────────────────────────────────────────
# FileTarget (JSONL)
# ─────────────────────────────────────────────

class TestFileTarget:
    @pytest.mark.asyncio
    async def test_writes_jsonl(self, tmp_path):
        path = str(tmp_path / "out.jsonl")
        target = FileTarget(FileTargetConfig(path=path, format=ArchiveFormat.JSONL))
        await target.open()
        for i in range(3):
            await target.send(_sample_event(offset=i))
        await target.close()

        lines = Path(path).read_text().strip().splitlines()
        assert len(lines) == 3
        for line in lines:
            doc = json.loads(line)
            assert doc["topic"] == "orders"

    @pytest.mark.asyncio
    async def test_append_mode(self, tmp_path):
        path = str(tmp_path / "append.jsonl")
        cfg = FileTargetConfig(path=path, format=ArchiveFormat.JSONL, append=True)

        for _ in range(2):
            target = FileTarget(cfg)
            await target.open()
            await target.send(_sample_event())
            await target.close()

        lines = Path(path).read_text().strip().splitlines()
        assert len(lines) == 2

    @pytest.mark.asyncio
    async def test_writes_avro(self, tmp_path):
        import fastavro, io
        path = str(tmp_path / "out.avro")
        target = FileTarget(FileTargetConfig(path=path, format=ArchiveFormat.AVRO))
        await target.open()
        await target.send(_sample_event())
        await target.close()

        with open(path, "rb") as f:
            records = list(fastavro.reader(f))
        assert len(records) == 1
        assert records[0]["topic"] == "orders"

    @pytest.mark.asyncio
    async def test_creates_parent_dirs(self, tmp_path):
        path = str(tmp_path / "deep" / "nested" / "out.jsonl")
        target = FileTarget(FileTargetConfig(path=path))
        await target.open()
        await target.send(_sample_event())
        await target.close()
        assert Path(path).exists()


# ─────────────────────────────────────────────
# HttpTarget
# ─────────────────────────────────────────────

class TestHttpTarget:
    @pytest.mark.asyncio
    async def test_sends_correct_payload(self, aiohttp_server, aiohttp_client):
        """Mock HTTP server checks the payload structure."""
        from aiohttp import web

        received = []

        async def handler(request):
            body = await request.json()
            received.append(body)
            return web.Response(status=200)

        app = web.Application()
        app.router.add_post("/events", handler)
        server = await aiohttp_server(app)

        url = f"http://127.0.0.1:{server.port}/events"
        target = HttpTarget(HttpTargetConfig(url=url))
        await target.open()
        await target.send(_sample_event())
        await target.close()

        assert len(received) == 1
        doc = received[0]
        assert doc["topic"] == "orders"
        assert doc["_replay"] is True
        assert doc["value"]["amount"] == 99.99

    @pytest.mark.asyncio
    async def test_retries_on_500(self, aiohttp_server):
        """Target should retry on 5xx responses."""
        from aiohttp import web

        call_count = [0]

        async def handler(request):
            call_count[0] += 1
            if call_count[0] < 3:
                return web.Response(status=500, text="server error")
            return web.Response(status=200)

        app = web.Application()
        app.router.add_post("/events", handler)
        server = await aiohttp_server(app)

        url = f"http://127.0.0.1:{server.port}/events"
        target = HttpTarget(HttpTargetConfig(url=url, max_retries=3))
        await target.open()
        await target.send(_sample_event())
        await target.close()

        assert call_count[0] == 3
