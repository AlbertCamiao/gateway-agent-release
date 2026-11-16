"""Read-only Codex completion fallback for when Stop hooks are untrusted."""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
import subprocess
from pathlib import Path
from typing import Awaitable, Callable


LOGGER = logging.getLogger("feishu-gateway")


_SCRIPT = r'''
import glob, json, os, sys
thread, turn = sys.argv[1], sys.argv[2]
roots = os.path.expanduser("~/.codex/sessions")
for path in sorted(glob.glob(os.path.join(roots, "**", "*-" + thread + ".jsonl"), recursive=True),
                   key=os.path.getmtime, reverse=True):
    try:
        stream = open(path, "r", encoding="utf-8", errors="replace")
    except OSError:
        continue
    with stream:
        for line in stream:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            payload = record.get("payload") if isinstance(record, dict) else None
            if not isinstance(payload, dict):
                continue
            if payload.get("turn_id") != turn:
                continue
            if record.get("type") == "event_msg" and payload.get("type") == "task_complete":
                print(json.dumps({"state": "completed", "output": payload.get("last_agent_message")}))
                raise SystemExit(0)
            if record.get("type") == "event_msg" and payload.get("type") == "task_failed":
                print(json.dumps({"state": "failed", "output": None}))
                raise SystemExit(0)
print("{}")
'''


def scan_codex_transcript(path: Path, thread_id: str, turn_id: str) -> dict | None:
    try:
        stream = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return None
    with stream:
        for line in stream:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            payload = record.get("payload") if isinstance(record, dict) else None
            if not isinstance(payload, dict) or payload.get("turn_id") != turn_id:
                continue
            if record.get("type") == "event_msg" and payload.get("type") == "task_complete":
                return {"state": "completed", "output": payload.get("last_agent_message")}
            if record.get("type") == "event_msg" and payload.get("type") == "task_failed":
                return {"state": "failed", "output": None}
    return None


class CodexCompletionWatcher:
    def __init__(
        self, transport, thread_id: str, turn_id: str, callback: Callable[[dict], Awaitable[None]],
        *, interval: float = 1.0, timeout: float = 600.0, runner=None,
    ) -> None:
        self.transport = transport
        self.thread_id = thread_id
        self.turn_id = turn_id
        self.callback = callback
        self.interval = interval
        self.timeout = timeout
        self._runner = runner or subprocess.run
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.completed = False

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    async def wait(self) -> None:
        if self._task is not None:
            await self._task

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.timeout
        while not self._stop.is_set() and loop.time() < deadline:
            try:
                result = await asyncio.to_thread(self._query)
            except Exception:
                LOGGER.exception("Codex completion watcher query failed")
                result = None
            if result is not None:
                LOGGER.info("Codex completion watcher observed ref=%s state=%s", self.turn_id, result.get("state"))
                await self.callback(result)
                self.completed = True
                return
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                return
            except TimeoutError:
                pass

    def _query(self) -> dict | None:
        command = self.transport._ssh_command(
            "exec " + shlex.join(("python3", "-I", "-c", _SCRIPT, self.thread_id, self.turn_id))
        )
        environment = {
            name: __import__("os").environ[name]
            for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
            if name in __import__("os").environ
        }
        try:
            result = self._runner(
                command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, timeout=15, check=False, env=environment,
            )
        except (OSError, subprocess.SubprocessError, TimeoutError):
            return None
        if getattr(result, "returncode", 1) != 0:
            return None
        try:
            payload = json.loads(result.stdout or "{}")
        except ValueError:
            return None
        if isinstance(payload, dict) and payload.get("state") in {"completed", "failed"}:
            return payload
        return None


_FIND_SCRIPT = r'''
import glob, json, os, sys
thread, client = sys.argv[1], sys.argv[2]
for path in sorted(glob.glob(os.path.join(os.path.expanduser("~/.codex/sessions"), "**", "*-" + thread + ".jsonl"), recursive=True), key=os.path.getmtime, reverse=True):
    try:
        stream = open(path, "r", encoding="utf-8", errors="replace")
    except OSError:
        continue
    with stream:
        for line in stream:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            payload = record.get("payload") if isinstance(record, dict) else None
            if not isinstance(payload, dict):
                continue
            item = payload.get("item")
            if not isinstance(item, dict):
                continue
            if item.get("type") == "UserMessage" and item.get("client_id") == client:
                turn = payload.get("turn_id")
                if isinstance(turn, str):
                    print(json.dumps({"turn_id": turn}))
                    raise SystemExit(0)
print("{}")
'''


def find_turn_by_client_id(transport, thread_id: str, client_id: str) -> str | None:
    command = transport._ssh_command(
        "exec " + shlex.join(("python3", "-I", "-c", _FIND_SCRIPT, thread_id, client_id))
    )
    environment = {
        name: __import__("os").environ[name]
        for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
        if name in __import__("os").environ
    }
    try:
        result = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=15, check=False, env=environment,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if getattr(result, "returncode", 1) != 0:
        return None
    try:
        payload = json.loads(result.stdout or "{}")
    except ValueError:
        return None
    turn = payload.get("turn_id") if isinstance(payload, dict) else None
    return turn if isinstance(turn, str) and turn else None
