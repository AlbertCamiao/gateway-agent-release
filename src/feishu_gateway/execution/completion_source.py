"""Completion transports and native-event normalization.

CompletionSource owns HTTP/DSH/Codex completion transports. It converts native
facts into CompletionEvent objects and never reads or writes task state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from typing import Any, AsyncIterator, Awaitable, Callable

from feishu_gateway.completion_server import CompletionServer
from feishu_gateway.contracts import CompletionEvent, SessionRef
from feishu_gateway.execution.codex_completion import CodexCompletionWatcher
from feishu_gateway.execution.completion_tunnel import CompletionTunnelManager


LOGGER = logging.getLogger("feishu-gateway")


class RuntimeCompletionSource:
    def __init__(
        self,
        *,
        home,
        dsh_transports,
        completion_port: int | None = None,
        completion_sink: Callable[[CompletionEvent], Awaitable[None]] | None = None,
        dsh_recovery: Callable[[], Awaitable[tuple[dict, ...]]] | None = None,
        codex_recovery: Callable[[], Awaitable[tuple[dict, ...]]] | None = None,
        watchdog_interval: float = 30.0,
        watchdog_grace: float = 120.0,
    ) -> None:
        self._home = home
        self._dsh_transports = tuple(dsh_transports)
        self._completion_port = completion_port
        self._completion_sink = completion_sink
        self._dsh_recovery = dsh_recovery
        self._codex_recovery = codex_recovery
        self._watchdog_interval = float(watchdog_interval)
        self._watchdog_grace = float(watchdog_grace)
        self._queue: asyncio.Queue[CompletionEvent | None] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._closed = False
        self._server: CompletionServer | None = None
        self._tunnel: CompletionTunnelManager | None = None
        self._tasks: list[asyncio.Task] = []
        self._watchers: list[CodexCompletionWatcher] = []
        self._watched_codex: set[tuple[str, str, str]] = set()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        token = secrets.token_hex(32)
        port_hint = self._completion_port if self._completion_port is not None else 0
        try:
            data = json.loads(self._home.completion_state_path.read_text())
            if isinstance(data.get("token"), str) and len(data["token"]) >= 32:
                token = data["token"]
            if self._completion_port is None and isinstance(data.get("port"), int):
                port_hint = data["port"]
        except (OSError, ValueError, TypeError):
            pass

        async def on_event(event: dict[str, Any]) -> None:
            await self._publish_http(event)

        self._server = CompletionServer(token, on_event, port=port_hint)
        try:
            port = await self._server.start()
        except OSError:
            LOGGER.warning(
                "Configured completion port %s is unavailable; using an ephemeral port.",
                port_hint,
            )
            self._server = CompletionServer(token, on_event)
            port = await self._server.start()
        self._write_completion_state(port, token)
        self._tunnel = CompletionTunnelManager(self._home, port)
        await self._tunnel.start()
        for transport in self._dsh_transports:
            self._tasks.append(asyncio.create_task(self._watch_dsh_completions(transport)))
        if self._dsh_transports and self._dsh_recovery is not None and self._watchdog_interval > 0:
            self._tasks.append(asyncio.create_task(self._watch_lost_completions()))
        if self._codex_recovery is not None and self._watchdog_interval > 0:
            self._tasks.append(asyncio.create_task(self._watch_codex_recovery()))

    def set_completion_sink(self, sink) -> None:
        self._completion_sink = sink

    def set_dsh_recovery(self, recovery) -> None:
        self._dsh_recovery = recovery

    async def events(self) -> AsyncIterator[CompletionEvent]:
        while True:
            event = await self._queue.get()
            if event is None:
                return
            yield event

    async def _publish(self, event: CompletionEvent) -> None:
        if self._closed:
            raise RuntimeError("completion source is closed")
        await self._queue.put(event)

    async def _publish_http(self, raw: dict[str, Any]) -> None:
        event_id = raw.get("event_id") or raw.get("eventId")
        state = raw.get("state")
        session_id = raw.get("session_id") or raw.get("sessionId")
        turn_id = raw.get("turn_id") or raw.get("turnId")
        task_id = raw.get("task_id") or raw.get("taskId")
        if (
            not isinstance(event_id, str) or not event_id
            or state not in {"completed", "failed"}
            or not isinstance(session_id, str) or not session_id
        ):
            return
        execution_ref = (
            raw.get("execution_ref") or raw.get("requestId")
            or (turn_id if isinstance(turn_id, str) and turn_id else None)
        )
        if not isinstance(execution_ref, str) or not execution_ref:
            return
        source = raw.get("source")
        runtime = raw.get("runtime")
        if not isinstance(runtime, str) or not runtime:
            runtime = "dsh" if source in {"dsh", "dsh-bridge"} else "codex"
        host = raw.get("host")
        if not isinstance(host, str) or not host:
            if source == "dsh-bridge":
                LOGGER.warning("DSH completion event is missing host; dropping it")
                return
            host = "local"
        output = raw.get("output") or raw.get("last_assistant_message")
        event = CompletionEvent(
            event_id=event_id,
            target=SessionRef(host, runtime, session_id),
            execution_ref=execution_ref,
            state=state,
            occurred_at=time.time(),
            output=output if isinstance(output, str) else None,
        )
        if self._completion_sink is not None:
            await self._completion_sink(event)
        else:
            await self._publish(event)

    def _write_completion_state(self, port: int, token: str) -> None:
        self._home.completion_state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = self._home.completion_state_path.with_name(
            self._home.completion_state_path.name + ".tmp",
        )
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump({"host": "127.0.0.1", "port": port, "token": token}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self._home.completion_state_path)
        os.chmod(self._home.completion_state_path, 0o600)

    async def _watch_lost_completions(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(self._watchdog_interval)
                await self._scan_lost_completions()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("DSH completion watchdog stopped")

    async def _scan_lost_completions(self) -> None:
        if self._dsh_recovery is None:
            return
        now = time.time()
        candidates = await self._dsh_recovery()
        for candidate in candidates:
            target = candidate.get("target")
            execution_ref = candidate.get("execution_ref")
            session_id = candidate.get("session_id")
            workspace = candidate.get("workspace")
            updated = candidate.get("updated_at")
            if (
                not isinstance(target, SessionRef)
                or not isinstance(execution_ref, str) or not execution_ref
                or not isinstance(session_id, str) or not session_id
                or not isinstance(workspace, str)
                or not isinstance(updated, (int, float))
                or now - float(updated) < self._watchdog_grace
            ):
                continue
            transport = next(
                (item for item in self._dsh_transports if str(item.host) == target.host),
                None,
            )
            if transport is None:
                continue
            try:
                observation = await transport.observe(
                    session_id, execution_ref, cwd=workspace,
                )
            except Exception:
                observation = None
            if observation is None or observation.status not in {"completed", "failed"}:
                continue
            await self._publish(CompletionEvent(
                event_id=f"{session_id}:watchdog:{observation.status}:{updated}",
                target=target,
                execution_ref=execution_ref,
                state=observation.status,
                occurred_at=now,
                output=observation.text,
            ))

    async def _watch_dsh_completions(self, transport) -> None:
        host = str(getattr(transport, "host", "") or "")

        async def on_line(line: str) -> None:
            try:
                await self._ingest_dsh_line(line, host=host)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("DSH completion line could not be processed (host=%s)", host)

        backoff = 1.0
        while not self._stop.is_set():
            try:
                await transport.watch_completions(on_line)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOGGER.warning(
                    "DSH completion feed stopped; reconnecting (host=%s, %s)",
                    host, type(error).__name__,
                )
            else:
                LOGGER.warning("DSH completion feed ended; reconnecting (host=%s).", host)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                return
            except TimeoutError:
                pass
            backoff = min(backoff * 2, 30.0)

    async def _ingest_dsh_line(self, line: str, *, host: str = "local") -> None:
        try:
            record = json.loads(line)
        except (ValueError, TypeError):
            return
        if not isinstance(record, dict):
            return
        event_id = record.get("eventId")
        session_id = record.get("sessionId")
        request_id = record.get("requestId")
        if (
            not isinstance(event_id, str) or not event_id
            or not isinstance(session_id, str) or not session_id
            or not isinstance(request_id, str) or not request_id
        ):
            return
        reason = str(record.get("reason") or "")
        state = "completed" if reason == "completed" else "failed"
        text = record.get("text")
        await self._publish(CompletionEvent(
            event_id=event_id,
            target=SessionRef(host, "dsh", session_id),
            execution_ref=request_id,
            state=state,
            occurred_at=time.time(),
            output=text if isinstance(text, str) else None,
        ))

    def _watch_codex(self, *, target: SessionRef, execution_ref: str, transport) -> None:
        LOGGER.info(
            "Codex completion watcher starting for %s/%s ref=%s",
            target.host, target.session_id, execution_ref,
        )
        async def complete(result: dict) -> None:
            state = str(result.get("state") or "failed")
            if state not in {"completed", "failed"}:
                return
            await self._publish(CompletionEvent(
                event_id=f"{target.session_id}:codex:{execution_ref}",
                target=target,
                execution_ref=execution_ref,
                state=state,
                occurred_at=time.time(),
                output=result.get("output") if isinstance(result.get("output"), str) else None,
            ))

        watcher = CodexCompletionWatcher(
            transport, target.session_id, execution_ref, complete,
        )
        self._watchers.append(watcher)
        self._tasks.append(asyncio.create_task(watcher.start()))

    async def _watch_codex_recovery(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self._watchdog_interval)
                for candidate in await self._codex_recovery():
                    target = candidate.get("target")
                    ref = candidate.get("execution_ref")
                    transport = candidate.get("transport")
                    if (
                        isinstance(target, SessionRef)
                        and isinstance(ref, str) and ref
                        and transport is not None
                    ):
                        key = (target.host, target.session_id, ref)
                        if key in self._watched_codex:
                            continue
                        self._watched_codex.add(key)
                        self._watch_codex(
                            target=target, execution_ref=ref, transport=transport,
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Codex completion recovery scan failed")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        for watcher in self._watchers:
            await watcher.close()
        self._watchers = []
        if self._tunnel is not None:
            await self._tunnel.close()
            self._tunnel = None
        if self._server is not None:
            await self._server.close()
            self._server = None
        await self._queue.put(None)
