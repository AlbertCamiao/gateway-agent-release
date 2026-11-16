"""Loopback completion receiver for Codex/DSH hooks."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

_MAX_HEADER = 8192
_MAX_BODY = 262144
LOGGER = logging.getLogger("feishu-gateway")


class CompletionServer:
    def __init__(
        self, token: str, on_event: Callable[[dict[str, Any]], Awaitable[None]],
        *, host: str = "127.0.0.1", port: int = 0,
    ) -> None:
        if not token or len(token) > 256:
            raise ValueError("completion token must be non-empty and bounded")
        self._token = token
        self._on_event = on_event
        self._host = host
        self._port = port
        self._server: asyncio.AbstractServer | None = None

    @property
    def port(self) -> int:
        return self._port

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._serve, self._host, self._port)
        self._port = int(self._server.sockets[0].getsockname()[1])
        return self._port

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            if len(header) > _MAX_HEADER:
                return await self._reply(writer, 431)
            lines = header.decode("latin-1").split("\r\n")
            parts = lines[0].split(" ")
            if len(parts) != 3 or parts[0] != "POST" or parts[1] != "/completion":
                return await self._reply(writer, 404)
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            if headers.get("x-gateway-token") != self._token:
                return await self._reply(writer, 401)
            try:
                length = int(headers.get("content-length", "0"))
            except ValueError:
                return await self._reply(writer, 400)
            if length <= 0 or length > _MAX_BODY:
                return await self._reply(writer, 413)
            body = await asyncio.wait_for(reader.readexactly(length), 5)
            try:
                event = json.loads(body)
            except (ValueError, RecursionError):
                return await self._reply(writer, 400)
            if not isinstance(event, dict):
                return await self._reply(writer, 400)
            try:
                await self._on_event(event)
            except Exception:
                LOGGER.exception("completion event handler failed")
                return await self._reply(writer, 500)
            await self._reply(writer, 202)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    @staticmethod
    async def _reply(writer: asyncio.StreamWriter, status: int) -> None:
        reason = {202: "Accepted", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 413: "Payload Too Large", 431: "Request Header Fields Too Large", 500: "Internal Server Error"}.get(status, "Error")
        writer.write(f"HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode())
        await writer.drain()
