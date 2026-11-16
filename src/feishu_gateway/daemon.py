"""R1.1 Gateway daemon skeleton.

This module is intentionally additive: it does not wire the existing Feishu
runtime. It provides a private Unix-socket daemon and a thin CLI client so
later gates can move Operations behind the daemon without changing the legacy
entry point yet.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import http.client
import importlib
import json
import logging
import os
import shlex
import signal
import socket
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from feishu_gateway.daemon_config import ConfigWatcher, DaemonConfigError, read_daemon_config
from feishu_gateway.home import GatewayHome
from feishu_gateway.operation_dispatch import OperationDispatcher


VERSION = "0.1.0"
_MAX_HEADER = 8192
_MAX_BODY = 65536


class DaemonError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DaemonPaths:
    socket: Path
    lock: Path
    pid: Path
    log: Path

    @classmethod
    def from_home(cls, home: GatewayHome | None = None) -> "DaemonPaths":
        current = home or GatewayHome.discover()
        return cls(
            current.daemon_socket_path,
            current.daemon_lock_path,
            current.daemon_pid_path,
            current.daemon_log_path,
        )


def _session_snapshot_state(value: object) -> str:
    state = str(value or "").lower()
    return state if state in {"idle", "busy", "offline"} else "unknown"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)


def _write_private(path: Path, data: bytes) -> None:
    _ensure_parent(path)
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(path, 0o600)
    finally:
        if descriptor >= 0:
            with contextlib.suppress(OSError):
                os.close(descriptor)


@contextlib.contextmanager
def _daemon_lock(path: Path):
    _ensure_parent(path)
    descriptor = os.open(
        path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600,
    )
    try:
        entry = os.fstat(descriptor)
        if (
            not stat.S_ISREG(entry.st_mode)
            or entry.st_uid != os.getuid()
            or stat.S_IMODE(entry.st_mode) != 0o600
        ):
            raise DaemonError("daemon lock must be a private 0600 file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise DaemonError("Gateway daemon is already running.") from None
        except OSError as error:
            raise DaemonError("Could not acquire the Gateway daemon lock.") from error
        yield
    finally:
        with contextlib.suppress(OSError):
            os.close(descriptor)


def _clear_stale_socket(path: Path) -> None:
    try:
        entry = path.lstat()
    except FileNotFoundError:
        return
    if (
        not stat.S_ISSOCK(entry.st_mode)
        or entry.st_uid != os.getuid()
        or stat.S_IMODE(entry.st_mode) & 0o077
    ):
        raise DaemonError("refusing to replace an unsafe daemon socket")
    path.unlink()


def _peer_uid(writer: asyncio.StreamWriter) -> int | None:
    sock = writer.get_extra_info("socket")
    if sock is None:
        return None
    try:
        if hasattr(socket, "LOCAL_PEERCRED"):
            raw = sock.getsockopt(0, socket.LOCAL_PEERCRED, 12)
            return int.from_bytes(raw[4:8], sys.byteorder)
        if hasattr(socket, "SO_PEERCRED"):
            raw = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            return int.from_bytes(raw[4:8], sys.byteorder)
    except (OSError, AttributeError):
        return None
    return None


class DaemonServer:
    def __init__(
        self, paths: DaemonPaths, dispatcher: OperationDispatcher | None = None, *,
        runtime=None, ask_runtime=None, ingress_manager=None, config_watcher=None,
    ) -> None:
        self.paths = paths
        self.dispatcher = dispatcher
        self.runtime = runtime
        self.ask_runtime = ask_runtime
        self.ingress_manager = ingress_manager
        self.config_watcher = config_watcher
        self._ingress_task: asyncio.Task | None = None
        self._config_task: asyncio.Task | None = None
        self._runtime_task: asyncio.Task | None = None
        self.ready = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.started_at = time.time()
        self.pid = os.getpid()
        self._server: asyncio.AbstractServer | None = None
        self._stack: contextlib.ExitStack | None = None
        self._socket_owned = False

    async def serve(self) -> None:
        self._stack = contextlib.ExitStack()
        try:
            self._stack.enter_context(_daemon_lock(self.paths.lock))
            if len(str(self.paths.socket).encode("utf-8")) >= 104:
                raise DaemonError("daemon socket path is too long")
            _ensure_parent(self.paths.socket)
            _clear_stale_socket(self.paths.socket)
            self._server = await asyncio.start_unix_server(
                self._handle, path=str(self.paths.socket),
            )
            self._socket_owned = True
            os.chmod(self.paths.socket, 0o600)
            _write_private(
                self.paths.pid,
                json.dumps({"pid": self.pid, "started_at": self.started_at}).encode("utf-8"),
            )
            self.ready.set()
            # Start the task runtime before any task arrives so completion
            # events and durable inbox entries are received after a restart.
            if self.runtime is not None:
                starter = getattr(self.runtime, "start", None)
                if starter is not None:
                    self._runtime_task = asyncio.create_task(self._start_runtime(starter))
            if self.ingress_manager is not None:
                runner = getattr(self.ingress_manager, "run", self.ingress_manager.start)
                self._ingress_task = asyncio.create_task(runner())
            if self.config_watcher is not None:
                self._config_task = asyncio.create_task(self.config_watcher.run(self.stop_event))
            await self.stop_event.wait()
        finally:
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
            if self._socket_owned:
                with contextlib.suppress(FileNotFoundError):
                    if self.paths.socket.lstat().st_uid == os.getuid():
                        self.paths.socket.unlink()
                self._socket_owned = False
            with contextlib.suppress(FileNotFoundError):
                self.paths.pid.unlink()
            if self._stack is not None:
                self._stack.close()
                self._stack = None
            if self._config_task is not None:
                self._config_task.cancel()
                await asyncio.gather(self._config_task, return_exceptions=True)
                self._config_task = None
            if self._ingress_task is not None:
                self._ingress_task.cancel()
                await asyncio.gather(self._ingress_task, return_exceptions=True)
                self._ingress_task = None
            if self.ingress_manager is not None:
                await self.ingress_manager.stop()
            if self._runtime_task is not None:
                self._runtime_task.cancel()
                await asyncio.gather(self._runtime_task, return_exceptions=True)
                self._runtime_task = None
            if self.runtime is not None:
                await self.runtime.close()

    async def _start_runtime(self, starter) -> None:
        try:
            await starter()
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger("feishu-gateway").warning(
                "Task runtime did not start at daemon startup; "
                "completion receiving will retry on the first task.",
            )

    def stop(self) -> None:
        self.stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "pid": self.pid,
            "version": VERSION,
            "socket": str(self.paths.socket),
            "started_at": self.started_at,
            "rpc": self.dispatcher is not None,
            "config_error": self.config_watcher.error if self.config_watcher is not None else None,
        }

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            uid = _peer_uid(writer)
            if uid is not None and uid != os.getuid():
                await self._reply(writer, 403, {"ok": False, "error": {"code": "forbidden"}})
                return
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
            if len(header) > _MAX_HEADER:
                await self._reply(writer, 431, {"ok": False, "error": {"code": "header_too_large"}})
                return
            lines = header.decode("latin-1").split("\r\n")
            parts = lines[0].split(" ")
            if len(parts) != 3:
                await self._reply(writer, 400, {"ok": False, "error": {"code": "bad_request"}})
                return
            method, path, _ = parts
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.strip().lower()] = value.strip()
            try:
                length = int(headers.get("content-length", "0"))
            except ValueError:
                await self._reply(writer, 400, {"ok": False, "error": {"code": "bad_length"}})
                return
            if length < 0 or length > _MAX_BODY:
                await self._reply(writer, 413, {"ok": False, "error": {"code": "body_too_large"}})
                return
            body = await asyncio.wait_for(reader.readexactly(length), timeout=5) if length else b""
            await self._route(writer, method, path, body)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, OSError):
            pass
        except Exception:
            logging.getLogger("feishu-gateway").exception("daemon request failed")
            with contextlib.suppress(Exception):
                await self._reply(writer, 500, {"ok": False, "error": {"code": "internal_error"}})
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    async def _route(
        self, writer: asyncio.StreamWriter, method: str, path: str, body: bytes,
    ) -> None:
        if method == "GET" and path == "/ingress/status":
            if self.ingress_manager is not None:
                await self._reply(writer, 200, self.ingress_manager.status())
            else:
                await self._reply(writer, 200, {
                    "enabled": False, "connected": False, "bound": False,
                    "app_id": None, "chat_type": None, "last_error": None,
                })
            return
        if method == "POST" and path == "/ingress/bind":
            if self.ingress_manager is None:
                await self._reply(writer, 409, {"ok": False, "error": {"code": "ingress_disabled"}})
                return
            code = self.ingress_manager.bind()
            if code is None:
                await self._reply(writer, 409, {"ok": False, "error": {"code": "ingress_unavailable"}})
                return
            await self._reply(writer, 200, {"ok": True, "code": code, "ttl": 600})
            return
        if method == "GET" and path == "/health":
            await self._reply(writer, 200, self.health())
            return
        if method == "POST" and path == "/control/shutdown":
            payload = self._json(body)
            if payload.get("confirm") is not True:
                await self._reply(writer, 400, {"ok": False, "error": {"code": "confirmation_required"}})
                return
            await self._reply(writer, 200, {"ok": True, "stopping": True})
            self.stop()
            return
        if method == "POST" and path == "/ask":
            if self.ask_runtime is None:
                await self._reply(writer, 501, {"ok": False, "error": {"code": "ask_unavailable"}})
                return
            payload = self._json(body)
            if not payload or not isinstance(payload.get("text"), str):
                await self._reply(writer, 400, {"ok": False, "error": {"code": "invalid_args"}})
                return
            result = await self.ask_runtime.ask(
                str(payload["text"]), origin=str(payload.get("origin") or "cli"),
            )
            await self._reply(writer, 200, result.as_dict())
            return
        if method == "POST" and path == "/rpc":
            if self.dispatcher is None:
                await self._reply(
                    writer, 501,
                    {"ok": False, "error": {"code": "rpc_not_implemented", "message": "dispatcher is not configured"}},
                )
                return
            payload = self._json(body)
            if not payload:
                await self._reply(writer, 400, {"ok": False, "error": {"code": "invalid_operation"}})
                return
            result = await self.dispatcher.dispatch_payload(payload)
            await self._reply(writer, 200, result.as_dict())
            return
        await self._reply(writer, 404, {"ok": False, "error": {"code": "not_found"}})

    @staticmethod
    def _json(body: bytes) -> Mapping[str, Any]:
        try:
            value = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeError):
            return {}
        return value if isinstance(value, Mapping) else {}

    @staticmethod
    async def _reply(writer: asyncio.StreamWriter, status: int, payload: Mapping[str, Any]) -> None:
        reasons = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 413: "Payload Too Large", 431: "Request Header Fields Too Large", 500: "Internal Server Error", 501: "Not Implemented"}
        body = json.dumps(dict(payload), separators=(",", ":")).encode("utf-8")
        writer.write(
            f"HTTP/1.1 {status} {reasons.get(status, 'Error')}\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n".encode("ascii") + body
        )
        await writer.drain()


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path: Path, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._path = str(path)

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


class DaemonClient:
    def __init__(self, paths: DaemonPaths) -> None:
        self.paths = paths

    def request(
        self, method: str, path: str, payload: Mapping[str, Any] | None = None,
        *, timeout: float = 5.0,
    ) -> dict[str, Any]:
        connection = _UnixHTTPConnection(self.paths.socket, timeout)
        body = json.dumps(dict(payload), separators=(",", ":")).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if body is not None else {}
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read()
        except TimeoutError as error:
            raise DaemonError("Gateway daemon request timed out.") from error
        except (OSError, http.client.HTTPException) as error:
            raise DaemonError("Gateway daemon is not running.") from error
        finally:
            connection.close()
        try:
            result = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeError):
            raise DaemonError("Gateway daemon returned invalid data.") from None
        if not isinstance(result, dict):
            raise DaemonError("Gateway daemon returned invalid data.")
        if response.status >= 400:
            error = result.get("error") if isinstance(result.get("error"), dict) else {}
            raise DaemonError(str(error.get("message") or error.get("code") or f"HTTP {response.status}"))
        return result

    def health(self) -> dict[str, Any]:
        return self.request("GET", "/health", timeout=2.0)

    def shutdown(self) -> dict[str, Any]:
        return self.request("POST", "/control/shutdown", {"confirm": True}, timeout=5.0)

    def ask(self, text: str, *, origin: str = "cli", timeout: float = 60.0) -> dict[str, Any]:
        return self.request("POST", "/ask", {"text": text, "origin": origin}, timeout=timeout)

    def ingress_status(self) -> dict[str, Any]:
        return self.request("GET", "/ingress/status", None)

    def ingress_bind(self) -> dict[str, Any]:
        return self.request("POST", "/ingress/bind", {})

    def rpc(
        self, operation: str, args: Mapping[str, object] | None = None,
        *, origin: str = "cli", confirmed: bool = False,
        request_id: str | None = None, timeout: float = 30.0,
    ) -> dict[str, Any]:
        payload = {
            "request_id": request_id or os.urandom(16).hex(),
            "name": operation,
            "args": dict(args or {}),
            "origin": origin,
            "confirmed": confirmed,
        }
        return self.request("POST", "/rpc", payload, timeout=timeout)


def _status(paths: DaemonPaths) -> tuple[bool, dict[str, Any] | None]:
    try:
        return True, DaemonClient(paths).health()
    except DaemonError:
        return False, None


def _wait_ready(paths: DaemonPaths, timeout: float = 5.0) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        running, health = _status(paths)
        if running and health is not None:
            return health
        time.sleep(0.1)
    return None


def _wait_stopped(paths: DaemonPaths, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        running, _ = _status(paths)
        if not running:
            return True
        time.sleep(0.1)
    return False


def _start(paths: DaemonPaths) -> int:
    GatewayHome.discover().ensure_dirs()
    running, health = _status(paths)
    if running and health is not None:
        print(f"Gateway daemon is already running (PID {health.get('pid')}).")
        return 0
    _ensure_parent(paths.log)
    log_descriptor = os.open(
        paths.log, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600,
    )
    source = str(Path(__file__).resolve().parents[1])
    env = dict(os.environ)
    env["PYTHONPATH"] = source + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "feishu_gateway.daemon", "run"],
            env=env, stdout=log_descriptor, stderr=log_descriptor,
            start_new_session=True, close_fds=True,
        )
    except OSError as error:
        print(f"Could not start Gateway daemon: {error}", file=sys.stderr)
        return 1
    finally:
        os.close(log_descriptor)
    health = _wait_ready(paths)
    if health is None:
        print("Gateway daemon did not become ready.", file=sys.stderr)
        print(f"Log: {paths.log}", file=sys.stderr)
        with contextlib.suppress(OSError):
            process.terminate()
        return 1
    print(f"Gateway daemon started (PID {health.get('pid')}).")
    return 0


def _stop(paths: DaemonPaths) -> int:
    try:
        DaemonClient(paths).shutdown()
    except DaemonError:
        print("Gateway daemon is not running.")
        return 0
    if not _wait_stopped(paths):
        print("Gateway daemon did not stop.", file=sys.stderr)
        return 1
    print("Gateway daemon stopped.")
    return 0


def _status_command(paths: DaemonPaths) -> int:
    running, health = _status(paths)
    if not running or health is None:
        print("Gateway daemon: stopped.")
        return 1
    print(f"Gateway daemon: running (PID {health.get('pid')}).")
    print(f"Socket: {health.get('socket')}.")
    return 0


def _console(paths: DaemonPaths) -> int:
    from feishu_gateway.cli.console import choose_backend

    if choose_backend() == "textual":
        try:
            from feishu_gateway.cli.console.client import run
        except ImportError:
            pass
        else:
            return run(paths)
    return _line_console(paths)


def _line_console(paths: DaemonPaths) -> int:
    running, health = _status(paths)
    if not running or health is None:
        print("Gateway daemon is not running. Run: gateway-agent start", file=sys.stderr)
        return 1
    print(f"Gateway console attached (PID {health.get('pid')}). Type 'exit' to detach.")
    while True:
        try:
            line = input("gateway> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        try:
            parts = shlex.split(line)
        except ValueError:
            parts = []
        if parts and parts[0] in {"exit", "quit"}:
            return 0
        if parts and parts[0] == "help":
            print("natural language | ask TEXT | task prompt HOST WORKSPACE SESSION TEXT")
            print("task status ID | task result ID | task resolve ID [REASON] | session list|status|refresh")
            print("workspace list|register|remove | host ... | dsh ... | connect ...")
            print("doctor | completion sync ... | op NAME JSON [--confirmed] | status | health | exit")
            continue
        if parts and parts[0] == "status":
            _status_command(paths)
            continue
        if parts and parts[0] == "health":
            current = _status(paths)
            print(json.dumps(current[1], ensure_ascii=False) if current[0] else "stopped")
            continue
        try:
            if parts and parts[0] == "ask":
                _console_ask(paths, " ".join(parts[1:]))
            elif parts and parts[0] == "task":
                _console_task(paths, parts[1:])
            elif parts and parts[0] == "session":
                _console_session(paths, parts[1:])
            elif parts and parts[0] == "workspace":
                _console_workspace(paths, parts[1:])
            elif parts and parts[0] == "host":
                _console_host(paths, parts[1:])
            elif parts and parts[0] == "dsh":
                _console_dsh(paths, parts[1:])
            elif parts and parts[0] == "connect":
                _console_connect(paths, parts[1:])
            elif parts and parts[0] == "doctor":
                _console_rpc(paths, "doctor", {
                    "host": parts[1] if len(parts) > 1 else None,
                    "workspace": parts[2] if len(parts) > 2 else None,
                })
            elif parts and parts[0] == "completion":
                _console_completion(paths, parts[1:])
            elif parts and parts[0] == "op":
                _console_op(paths, parts[1:])
            else:
                _console_ask(paths, line)
        except (ValueError, json.JSONDecodeError) as error:
            print(f"console command error: {error}", file=sys.stderr)


def _console_ask(paths: DaemonPaths, text: str) -> None:
    if not text:
        return
    try:
        result = DaemonClient(paths).ask(text, origin="console")
    except DaemonError as error:
        print(str(error), file=sys.stderr)
        return
    _print_console_result(result)


def _console_op(paths: DaemonPaths, parts: list[str]) -> None:
    if not parts:
        raise ValueError("op requires OPERATION [JSON]")
    confirmed = "--confirmed" in parts
    parts = [part for part in parts if part != "--confirmed"]
    operation = parts[0]
    args = json.loads(parts[1]) if len(parts) > 1 else {}
    if not isinstance(args, dict):
        raise ValueError("op args must be a JSON object")
    _console_rpc(paths, operation, args, confirmed=confirmed)


def _console_task(paths: DaemonPaths, parts: list[str]) -> None:
    if not parts:
        raise ValueError("task requires prompt|status|result")
    action = parts[0]
    if action == "prompt":
        if len(parts) < 5:
            raise ValueError("task prompt HOST WORKSPACE SESSION TEXT")
        _console_rpc(paths, "task.prompt", {
            "host": parts[1], "workspace": parts[2], "session": parts[3],
            "text": " ".join(parts[4:]),
        })
    elif action in {"status", "result"} and len(parts) == 2:
        _console_rpc(paths, f"task.{action}", {"task_id": parts[1]})
    elif action == "resolve" and len(parts) >= 2:
        args = {"task_id": parts[1]}
        if len(parts) > 2:
            args["reason"] = " ".join(parts[2:])
        _console_rpc(paths, "task.resolve", args, confirmed=True)
    else:
        raise ValueError("task status ID | task result ID | task resolve ID [REASON]")


def _console_session(paths: DaemonPaths, parts: list[str]) -> None:
    if not parts:
        raise ValueError("session requires list|status|refresh")
    if parts[0] == "list":
        args = {"host": parts[1]} if len(parts) > 1 else {}
        _console_rpc(paths, "session.list", args)
    elif parts[0] == "status" and len(parts) == 3:
        _console_rpc(paths, "session.status", {"host": parts[1], "session": parts[2]})
    elif parts[0] == "refresh" and len(parts) >= 2:
        args = {"host": parts[1]}
        if len(parts) > 2:
            args["workspace"] = parts[2]
        _console_rpc(paths, "session.refresh", args)
    else:
        raise ValueError("session list [HOST] | session status HOST SESSION | session refresh HOST [WORKSPACE]")


def _console_workspace(paths: DaemonPaths, parts: list[str]) -> None:
    if not parts:
        raise ValueError("workspace requires list|register|remove")
    if parts[0] == "list":
        args = {"host": parts[1]} if len(parts) > 1 else {}
        _console_rpc(paths, "workspace.list", args)
    elif parts[0] in {"register", "remove"} and len(parts) == 3:
        _console_rpc(paths, f"workspace.{parts[0]}", {
            "host": parts[1], "workspace": parts[2],
        }, confirmed=True)
    else:
        raise ValueError("workspace list [HOST] | workspace register|remove HOST WORKSPACE")


def _console_host(paths: DaemonPaths, parts: list[str]) -> None:
    if not parts:
        raise ValueError("host requires list|candidates|add|remove")
    if parts[0] in {"list", "candidates"}:
        _console_rpc(paths, f"host.{parts[0]}", {})
    elif parts[0] == "remove" and len(parts) == 2:
        _console_rpc(paths, "host.remove", {"host": parts[1]}, confirmed=True)
    else:
        raise ValueError("host list|candidates|remove HOST; use op host.add JSON for add")


def _console_dsh(paths: DaemonPaths, parts: list[str]) -> None:
    if len(parts) < 2:
        raise ValueError("dsh status|plan|connect HOST [--profile NAME]")
    action, host = parts[0], parts[1]
    args = {"host": host}
    if "--profile" in parts:
        index = parts.index("--profile")
        if index + 1 < len(parts):
            args["profile"] = parts[index + 1]
    confirmed = action == "connect"
    _console_rpc(paths, f"dsh.{action}", args, confirmed=confirmed)


def _console_connect(paths: DaemonPaths, parts: list[str]) -> None:
    if not parts:
        raise ValueError("connect HOST [--profile NAME] [--no-dsh] [--no-codex]")
    args = {"host": parts[0]}
    if "--profile" in parts:
        index = parts.index("--profile")
        if index + 1 < len(parts):
            args["profile"] = parts[index + 1]
    args["no_dsh"] = "--no-dsh" in parts
    args["no_codex"] = "--no-codex" in parts
    _console_rpc(paths, "connect", args, confirmed=True)


def _console_completion(paths: DaemonPaths, parts: list[str]) -> None:
    if len(parts) < 2 or parts[0] != "sync":
        raise ValueError("completion sync HOST [--no-dsh]")
    _console_rpc(paths, "completion.sync", {
        "host": parts[1], "dsh": "--no-dsh" not in parts,
    }, confirmed=True)


def _console_rpc(paths: DaemonPaths, operation: str, args: dict, *, confirmed: bool = False) -> None:
    try:
        result = DaemonClient(paths).rpc(operation, args, origin="console", confirmed=confirmed)
    except DaemonError as error:
        print(str(error), file=sys.stderr)
        return
    _print_console_result(result)


def _print_console_result(result: dict) -> None:
    if not result.get("ok"):
        error = result.get("error") if isinstance(result.get("error"), dict) else {}
        print(f"{error.get('code', 'operation_failed')}: {error.get('message', '')}", file=sys.stderr)
        return
    print(json.dumps(result.get("data") or {}, ensure_ascii=False, indent=2))

def _run(paths: DaemonPaths) -> int:
    from feishu_gateway.execution.task_runtime import TaskRuntime
    from feishu_gateway.management_operations import build_management_dispatcher

    home = GatewayHome.discover()
    home.ensure_dirs()

    async def runner() -> None:
        from feishu_gateway import config
        from feishu_gateway.discovery import scan_session_rows
        from feishu_gateway.execution.ask_runtime import (
            AskPlanner, AskRuntime, CatalogRows, CatalogUnavailable,
        )
        from feishu_gateway.execution.brief_planner import BriefPlanner
        from feishu_gateway.execution.prompt_formatter import PromptFormatter, read_prompt_template
        from feishu_gateway.planner.model import StructuredModel
        from feishu_gateway.workspaces.registry import read_registry

        try:
            initial_config = read_daemon_config(home.daemon_config_path)
        except DaemonConfigError:
            initial_config = None
        scan_lock = asyncio.Lock()
        from feishu_gateway.execution.session_registry import SessionRegistry
        session_registry = SessionRegistry(home.daemon_data_dir / "session_registry.sqlite3")

        async def probe_session(host: str, workspace: str | None, session: str):
            from pathlib import PurePosixPath
            status = 0
            async with scan_lock:
                for attempt in range(3):
                    try:
                        registry = await asyncio.to_thread(read_registry, home.registry_path)
                        rows, status = await scan_session_rows(
                            host,
                            PurePosixPath(workspace) if workspace is not None else None,
                            registry=registry,
                        )
                        row = next(
                            (item for item in rows if item.get("session_id") == session),
                            None,
                        )
                        if row is None and workspace is not None:
                            fallback_rows, fallback_status = await scan_session_rows(
                                host, registry=registry,
                            )
                            status = max(status, fallback_status)
                            row = next(
                                (item for item in fallback_rows if item.get("session_id") == session),
                                None,
                            )
                        if status == 0 and rows:
                            session_registry.record(rows, host=host)
                        if row is not None:
                            return row, status
                    except Exception:
                        status = 2
                        session_registry.mark_host_stale(host)
                    if attempt < 2:
                        await asyncio.sleep(0.25 * (attempt + 1))
                stored = session_registry.get(host, session)
                if stored is not None:
                    return stored, max(status, 2)
                return None, status

        from feishu_gateway.discovery import codex_backends, workspace_scopes
        from feishu_gateway.execution.codex_completion import find_turn_by_client_id
        from feishu_gateway.execution.completion_source import RuntimeCompletionSource
        from feishu_gateway.executors.backends import codex_backend_for
        from feishu_gateway.executors.execution import RuntimeExecutionPort

        registry = await asyncio.to_thread(read_registry, home.registry_path)
        ssh_scopes = config.read_ssh_scopes(home.ssh_config_path)
        dsh_transports = config.read_dsh_transports(home.dsh_config_path, ssh_scopes)
        local_roots, effective_ssh, effective_dsh = workspace_scopes(
            registry, ssh_scopes, dsh_transports,
        )
        try:
            preview = config.read_config(home.config_path)
        except config.GatewayConfigError:
            preview = None
        backends, workspace_backends, _ = codex_backends(
            preview.codex_command if preview is not None else "codex",
            local_roots, effective_ssh,
        )

        def codex_lookup(host, cwd):
            return codex_backend_for(
                cwd, backends.get(host or "local", ()),
                workspace_backends.get(host or "local", ()),
            )

        def dsh_lookup(host, _cwd):
            return next((item for item in effective_dsh if item.host == host), None)

        from feishu_gateway.contracts import SessionRef, SessionSnapshot

        async def observe(target):
            row, _ = await probe_session(target.host, None, target.session_id)
            workspace = str((row or {}).get("cwd") or next(iter(registry.get(target.host, ())), "/"))
            state = _session_snapshot_state((row or {}).get("state"))
            return SessionSnapshot(
                target, workspace, row is not None, state, time.time(),
                (row or {}).get("title") if isinstance((row or {}).get("title"), str) else None,
            )

        async def catalog(hosts, runtimes, workspace):
            snapshots = []
            unavailable_hosts = set()
            unavailable_runtimes = set()
            for host in hosts or frozenset(registry):
                rows, status = await scan_session_rows(
                    host,
                    PurePosixPath(workspace) if workspace is not None else None,
                    registry=registry,
                )
                if status:
                    unavailable_hosts.add(host)
                    unavailable_runtimes.update((runtime, host) for runtime in runtimes)
                for row in rows:
                    runtime = str(row.get("runtime") or "")
                    cwd = row.get("cwd")
                    if (runtimes and runtime not in runtimes) or not cwd:
                        continue
                    snapshots.append(SessionSnapshot(
                        SessionRef(host, runtime, str(row.get("session_id") or "")),
                        str(cwd), True,
                        _session_snapshot_state(row.get("state")),
                        time.time(),
                        row.get("title") if isinstance(row.get("title"), str) else None,
                    ))
            return tuple(snapshots), frozenset(unavailable_hosts), frozenset(unavailable_runtimes)

        runtime = None

        async def codex_recovery():
            if runtime is None:
                return ()
            candidates = []
            for candidate in await runtime.codex_recovery_candidates():
                target = candidate.get("target")
                workspace = candidate.get("workspace")
                if not isinstance(target, SessionRef) or not isinstance(workspace, str):
                    continue
                backend = codex_lookup(target.host, workspace)
                transport = getattr(backend, "transport", None)
                if transport is not None:
                    candidates.append({**candidate, "transport": transport})
            return tuple(candidates)

        completion_source = RuntimeCompletionSource(
            home=home,
            dsh_transports=effective_dsh,
            completion_port=(
                initial_config.completion_port
                if initial_config is not None and initial_config.completion_port
                else None
            ),
            codex_recovery=codex_recovery,
        )
        execution_port = RuntimeExecutionPort(
            codex_lookup=codex_lookup,
            dsh_lookup=dsh_lookup,
            find_turn=find_turn_by_client_id,
            completion_source=completion_source,
            catalog=catalog,
            observe=observe,
        )
        async def lookup_via_port(host, workspace, session):
            snapshots, _, _ = await execution_port.list(
                hosts=frozenset({host}), runtimes=frozenset(), workspace=workspace,
            )
            for snapshot in snapshots:
                if snapshot.target.session_id == session:
                    return {
                        "session_id": snapshot.target.session_id,
                        "host": snapshot.target.host,
                        "runtime": snapshot.target.runtime,
                        "title": snapshot.title,
                        "cwd": snapshot.workspace,
                        "state": snapshot.state,
                    }
            return None

        runtime = TaskRuntime(
            home,
            execution_port=execution_port,
            registry=registry,
            session_lookup=lookup_via_port, session_probe=probe_session,
        )
        completion_source.set_completion_sink(runtime._persist_completion_event)
        completion_source.set_dsh_recovery(runtime._dsh_recovery_candidates)
        from feishu_gateway.execution.task_workflow import TaskRuntimeWorkflow
        workflow = TaskRuntimeWorkflow(
            home.daemon_data_dir / "dbos_task_workflow.sqlite3",
            runtime.dbos_prepare, runtime.dbos_dispatch,
            runtime.dbos_completion_persist,
            runtime.dbos_completion_release,
            runtime.dbos_completion_receipt,
        )
        runtime.set_completion_sink(workflow.complete_task)
        management = build_management_dispatcher(home)
        dispatcher = OperationDispatcher({
            **management.handlers, **runtime.handlers(), "task.prompt": workflow.run,
        })
        try:
            preview = config.read_config(home.config_path)
        except config.GatewayConfigError:
            preview = None
        try:
            from dotenv import dotenv_values
            file_env = {
                key: str(value)
                for key, value in dotenv_values(home.env_path).items()
                if value is not None
            }
        except Exception:
            file_env = {}
        router_env = {**file_env, **os.environ}
        router = StructuredModel.from_env(
            router_env, codex_command=preview.codex_command if preview is not None else None,
        )

        catalog_cache: list[dict] = []
        catalog_at = 0.0

        async def ask_catalog():
            nonlocal catalog_cache, catalog_at
            now = time.monotonic()
            if catalog_cache and now - catalog_at < 10.0:
                return catalog_cache
            rows: list[dict] = []
            unavailable_hosts: set[str] = set()
            unavailable_runtimes: set[str] = set()
            async with scan_lock:
                try:
                    registry = await asyncio.to_thread(read_registry, home.registry_path)
                except Exception:
                    logging.getLogger("feishu-gateway").exception(
                        "ask catalog registry read failed",
                    )
                    cached = session_registry.list_rows()
                    if cached:
                        return CatalogRows(
                            cached,
                            unavailable_hosts=frozenset(
                                str(row.get("host") or "") for row in cached
                            ),
                        )
                    raise CatalogUnavailable(
                        "会话目录暂时不可查，任务未投递；请稍后重试。",
                    ) from None
                if not registry:
                    cached = session_registry.list_rows()
                    logging.getLogger("feishu-gateway").warning(
                        "ask catalog registry empty; cached=%d", len(cached),
                    )
                    if cached:
                        return CatalogRows(
                            cached,
                            unavailable_hosts=frozenset(
                                str(row.get("host") or "") for row in cached
                            ),
                        )
                for host in registry:
                    status = 2
                    host_rows: list[dict] = []
                    for attempt in range(3):
                        try:
                            host_rows, status = await scan_session_rows(host, registry=registry)
                        except Exception:
                            logging.getLogger("feishu-gateway").exception(
                                "ask catalog scan failed for host %s", host,
                            )
                            status = 2
                        if host_rows or status == 0:
                            break
                        if attempt < 2:
                            await asyncio.sleep(0.5 * (attempt + 1))
                    if status == 0 and host_rows:
                        session_registry.record(host_rows, host=host)
                    elif status != 0:
                        session_registry.mark_host_stale(host)
                    rows.extend(host_rows)
                    if status != 0:
                        runtimes = {str(r.get("runtime") or "") for r in host_rows}
                        if not host_rows:
                            unavailable_hosts.add(host)
                            unavailable_runtimes.update(("codex", "dsh"))
                        elif "codex" not in runtimes:
                            unavailable_runtimes.add("codex")
                        elif "dsh" not in runtimes:
                            unavailable_runtimes.add("dsh")
            if rows:
                view = CatalogRows(
                    rows, unavailable_hosts=unavailable_hosts,
                    unavailable_runtimes=unavailable_runtimes,
                )
                catalog_cache = view
                catalog_at = now
                return view
            cached = session_registry.list_rows()
            logging.getLogger("feishu-gateway").warning(
                "ask catalog fallback live_rows=0 cached=%d", len(cached),
            )
            if cached:
                view = CatalogRows(
                    cached,
                    unavailable_hosts=frozenset(
                        str(row.get("host") or "") for row in cached
                    ),
                    unavailable_runtimes=unavailable_runtimes,
                )
                catalog_cache = view
                catalog_at = now
                return view
            logging.getLogger("feishu-gateway").warning(
                "ask catalog empty rows=0 hosts=%s runtimes=%s",
                sorted(unavailable_hosts), list(unavailable_runtimes),
            )
            if unavailable_hosts:
                raise CatalogUnavailable("会话目录暂时不可查，任务未投递；请稍后重试。")
            return CatalogRows([], unavailable_runtimes=unavailable_runtimes)

        async def validate_target(operation):
            # The planner already selected from the live catalog; the
            # dispatcher performs the exact lookup and owner start-turn.
            return True

        runtime.set_summarizer(BriefPlanner(router).summarize)
        runtime.set_prompt_formatter(PromptFormatter(router=router, template=read_prompt_template))
        from feishu_gateway.execution.focus_store import FocusStore
        from feishu_gateway.execution.memory_store import (
            MemoryCurator, MemoryRetriever, MemoryStore,
        )
        memory_store = MemoryStore(home.daemon_state_path)
        memory_curator = MemoryCurator(router, memory_store)
        memory_retriever = MemoryRetriever(router)

        async def load_memory(origin, delivery, text, focus):
            profiles = await asyncio.to_thread(memory_store.recent, origin, delivery)
            events = await asyncio.to_thread(memory_store.recent_events, origin, delivery)
            selected = await memory_retriever.select(text, focus, profiles, events)
            evidence = await asyncio.to_thread(memory_store.evidence, selected)
            return {"profiles": profiles, "evidence": evidence}

        async def record_memory(origin, delivery, evidence, brief, prompt):
            stored = session_registry.get(
                evidence.target.host, evidence.target.session_id,
            ) or {}
            await memory_curator.record(
                origin, delivery,
                host=evidence.target.host, runtime=evidence.target.runtime,
                session=evidence.target.session_id,
                title=stored.get("title") if isinstance(stored.get("title"), str) else None,
                task=prompt, state=brief.state, outcome=brief.outcome,
                source_task_id=evidence.task_id,
            )

        runtime.set_memory_sink(record_memory)
        ask_runtime = AskRuntime(
            dispatcher, AskPlanner(router, catalog=ask_catalog),
            validate_target=validate_target,
            focus_store=FocusStore(home.daemon_state_path),
            memory_lookup=load_memory,
        )
        from feishu_gateway.edge.ingress import IngressManager
        ingress_manager = IngressManager(
            home, ask=lambda text, **kwargs: ask_runtime.ask(text, origin="feishu", **kwargs),
        )
        runtime.set_result_sink(ingress_manager.notify_result)
        # All completion sinks, workflow entry points, and result/memory sinks
        # are wired before either consumer starts.
        await runtime.start()
        await completion_source.start()
        def apply_config(value) -> None:
            logging.getLogger("feishu-gateway").setLevel(value.log_level)

        watcher = ConfigWatcher(home.daemon_config_path, on_change=apply_config)
        watcher.load_once()
        server = DaemonServer(
            paths, dispatcher, runtime=runtime, ask_runtime=ask_runtime,
            ingress_manager=ingress_manager, config_watcher=watcher,
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, server.stop)
        await server.serve()

    try:
        # lark_oapi's WS client captures an event loop at import time; import
        # it before asyncio.run so optional Feishu ingress can start later.
        try:
            importlib.import_module("feishu_gateway.edge.feishu")
        except Exception:
            pass
        asyncio.run(runner())
    except DaemonError as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


def _rpc_command(paths: DaemonPaths, operation: str, args_json: str, confirmed: bool, origin: str) -> int:
    try:
        payload = json.loads(args_json)
    except ValueError:
        print("Invalid JSON args.", file=sys.stderr)
        return 2
    if not isinstance(payload, dict):
        print("RPC args must be a JSON object.", file=sys.stderr)
        return 2
    try:
        result = DaemonClient(paths).rpc(operation, payload, origin=origin, confirmed=confirmed)
    except DaemonError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


def cli(argv: list[str] | None = None, paths: DaemonPaths | None = None) -> int:
    parser = argparse.ArgumentParser(prog="gateway-agent daemon", description="R1.1/R1.2 Gateway daemon skeleton.")
    commands = parser.add_subparsers(dest="action", required=True)
    for name in ("start", "stop", "status", "console", "run"):
        commands.add_parser(name)
    rpc = commands.add_parser("rpc", help="Call one Operation directly (provisional).")
    rpc.add_argument("operation")
    rpc.add_argument("--args", default="{}", dest="args_json")
    rpc.add_argument("--confirmed", action="store_true")
    rpc.add_argument("--origin", default="cli")
    args = parser.parse_args(argv)
    current = paths or DaemonPaths.from_home()
    if args.action == "start":
        return _start(current)
    if args.action == "stop":
        return _stop(current)
    if args.action == "status":
        return _status_command(current)
    if args.action == "console":
        return _console(current)
    if args.action == "run":
        return _run(current)
    if args.action == "rpc":
        return _rpc_command(current, args.operation, args.args_json, args.confirmed, args.origin)
    return 2


def main() -> int:
    return cli()


if __name__ == "__main__":
    raise SystemExit(main())
