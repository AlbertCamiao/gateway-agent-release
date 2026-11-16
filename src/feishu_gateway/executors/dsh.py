"""Call an approved DSH Host bridge locally or over authenticated SSH."""

import asyncio
import json
import os
import re
import shlex
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import UUID

from feishu_gateway.executors.codex_app_server import PreviewError, _close_server
from feishu_gateway.contracts import ExecutionObservation
from feishu_gateway.catalog.hosts import SSHScope
from feishu_gateway.catalog.types import SessionCandidate
from feishu_gateway.contracts import SessionRef
from feishu_gateway.executors.codex_remote import SSHReadTransport


_BASE_KEYS = {"host", "profile", "bridge_dir"}
_LEGACY_KEYS = _BASE_KEYS | {"project_root", "session_id"}
_WORKSPACE_KEYS = _BASE_KEYS | {"workspaces"}
_PROFILE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_SHA256 = re.compile(r"[a-f0-9]{64}")
_METHODS = frozenset({"list", "state", "prompt", "wait", "events"})
_FOLLOW_COMPLETIONS = """
import sys, time
path = sys.argv[1]
import os
stream = open(path, "r", encoding="utf-8", errors="replace")
# Replay only a bounded recent backlog on startup, so a lost feed cannot
# strand a recent task without reprocessing the entire history.
try:
    size = os.path.getsize(path)
    stream.seek(max(0, size - 262144))
    if size > 262144:
        stream.readline()
except OSError:
    pass
while True:
    line = stream.readline()
    if line:
        sys.stdout.write(line)
        sys.stdout.flush()
    else:
        time.sleep(0.2)
"""
_RESULT_LIMIT = 8192
_LIST_LIMIT = 512 * 1024
_WORKSPACE_LIMIT = 32
_TEXT_LIMIT = 16 * 1024

# This subprocess uses the same private socket, scope and metadata checks on
# either host. The token is read and used there without entering stdio.
_BRIDGE_CLIENT = r'''
import hmac
import http.client
import json
import os
import re
import socket
import stat
import sys
from pathlib import Path
from uuid import UUID


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path, timeout):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


def require_private(info, is_kind, mode):
    if not is_kind(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != mode:
        raise PermissionError("insecure bridge file")


def read_private(directory, name, limit=1024):
    descriptor = os.open(directory / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        require_private(os.fstat(stream.fileno()), stat.S_ISREG, 0o600)
        contents = stream.read(limit + 1)
    if not 0 < len(contents) <= limit:
        raise ValueError("invalid bridge file")
    return contents[:-1] if contents.endswith(b"\n") else contents


def canonical_cwd(value):
    if not isinstance(value, str) or not value.isprintable() or len(value.encode()) > 1024:
        raise ValueError("invalid workspace path")
    path = Path(value)
    if not path.is_absolute() or not path.name or ".." in path.parts or str(path) != value or value.startswith("//"):
        raise ValueError("invalid workspace path")
    return value


def workspace_roots(value):
    if not isinstance(value, dict) or set(value) != {"roots"}:
        raise ValueError("invalid workspace roots")
    roots = value["roots"]
    if not isinstance(roots, list) or len(roots) > 32:
        raise ValueError("invalid workspace roots")
    result = {}
    for row in roots:
        if not isinstance(row, dict) or set(row) not in ({"path"}, {"path", "includeDescendants"}):
            raise ValueError("invalid workspace root")
        path = canonical_cwd(row["path"])
        include = row.get("includeDescendants", False)
        if type(include) is not bool or path in result:
            raise ValueError("invalid workspace root")
        result[path] = include
    return result


def in_workspace(cwd, roots):
    return any(cwd == path or (include and cwd.startswith(path + "/")) for path, include in roots.items())


def covers(available, requested):
    return all(any(
        in_workspace(path, {root: recursive}) and (not include or recursive)
        for root, recursive in available.items()
    ) for path, include in requested.items())


def valid_session(value):
    return isinstance(value, str) and value.startswith("session-") and is_request_id(value[8:])


def project_list(value, expected, roots):
    items = value["items"]
    if not isinstance(items, list) or len(items) > (1024 if roots else 1):
        raise ValueError("invalid bridge list")
    result = []
    seen = set()
    for item in items:
        title = item.get("title")
        session = item["sessionId"]
        if not valid_session(session) or session in seen or (title is not None and not isinstance(title, str)):
            raise ValueError("invalid bridge identity")
        seen.add(session)
        if title is not None and (not title.strip() or not title.isprintable() or len(title) > 250):
            title = None
        row = {"sessionId": session, "title": title}
        if roots:
            cwd = canonical_cwd(item["cwd"])
            flags = ("agentAvailable", "running")
            if not in_workspace(cwd, roots) or not all(type(item[key]) is bool for key in flags):
                raise ValueError("invalid bridge catalog scope")
            row.update(cwd=cwd, **{key: item[key] for key in flags})
        elif session != expected:
            raise ValueError("invalid fixed bridge identity")
        result.append(row)
    return {"items": result}


def project_state(value):
    counts = ("cursor", "queued", "pendingInput")
    flags = ("active", "running")
    if not (all(type(value[key]) is int and value[key] >= 0 for key in counts)
            and all(type(value[key]) is bool for key in flags)):
        raise ValueError("invalid bridge state")
    approval_pending = value.get("approvalPending", False)
    if type(approval_pending) is not bool or (approval_pending and not value["pendingInput"]):
        raise ValueError("invalid bridge approval state")
    return {**{key: value[key] for key in (*counts, *flags)}, "approvalPending": approval_pending}


def is_request_id(value):
    return isinstance(value, str) and str(UUID(value)) == value


def is_count(value):
    return type(value) is int and value >= 0


def project_events(value, request_id):
    cursor, records = value["cursor"], value["records"]
    turn_current = value.get("turnCurrent", False)
    if not is_count(cursor) or not isinstance(records, list) or len(records) > 10 or type(turn_current) is not bool:
        raise ValueError("invalid bridge event page")
    events = []
    for record in records:
        event_type, seq = record["type"], record["seq"]
        if not is_count(seq) or seq > cursor:
            raise ValueError("invalid event sequence")
        event = {"type": event_type, "seq": seq}
        if event_type == "user/message":
            if record["requestId"] != request_id:
                raise ValueError("invalid bridge request identity")
            event["requestId"] = request_id
            if "turn" in record:
                if not is_count(record["turn"]):
                    raise ValueError("invalid bridge input turn")
                event["turn"] = record["turn"]
        elif event_type in ("assistant/message", "turn/end"):
            if not is_count(record["turn"]):
                raise ValueError("invalid bridge turn")
            event["turn"] = record["turn"]
            if event_type == "assistant/message":
                digest = record.get("textSha256")
                if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                    raise ValueError("invalid bridge reply digest")
                event["textSha256"] = digest
                reply_text = record.get("text")
                if reply_text is not None:
                    if not isinstance(reply_text, str) or len(reply_text.encode()) > 8192:
                        raise ValueError("invalid bridge reply text")
                    event["text"] = reply_text
            if event_type == "turn/end":
                reason = record["reason"]
                if not isinstance(reason, str) or not re.fullmatch(r"[a-z][a-z-]{0,31}", reason):
                    raise ValueError("invalid bridge completion reason")
                event["reason"] = reason
        else:
            raise ValueError("invalid bridge event")
        events.append(event)
    return {"cursor": cursor, "records": events, "turnCurrent": turn_current}


def read_client_input():
    source = sys.stdin.buffer.read(100001)
    if len(source) > 100000:
        raise ValueError("invalid bridge request size")
    payload = json.loads(source)
    if not isinstance(payload, dict):
        raise ValueError("invalid bridge request")
    return payload


def load_request(method, fixed_session, directory, roots, payload):
    if method == "list":
        if payload:
            raise ValueError("invalid bridge list request")
        return {"method": "list"}, None
    request = {"method": method}
    if roots:
        session, cwd = payload.pop("sessionId"), canonical_cwd(payload.pop("workspace"))
        if not valid_session(session) or not in_workspace(cwd, roots):
            return request, "dsh_scope_mismatch"
        request["workspace"] = cwd
    else:
        session, cwd = fixed_session, None
    request["sessionId"] = session
    if method == "state":
        if payload:
            raise ValueError("invalid bridge state request")
        return request, None
    request_id = payload["requestId"]
    if not is_request_id(request_id):
        raise ValueError("invalid request identity")
    request["requestId"] = request_id
    if method == "prompt":
        text = payload["text"]
        if set(payload) != {"requestId", "text"} or not isinstance(text, str) or not text.strip():
            raise ValueError("invalid bridge prompt")
        if len(text.encode()) > 16384:
            raise ValueError("invalid bridge prompt length")
        request["text"] = text
        return request, None
    if method not in ("wait", "events") or set(payload) != {"requestId", "expectedSha256"}:
        raise ValueError("unsupported bridge operation")
    if method == "wait":
        request["timeoutMs"] = 25000
    expected = payload["expectedSha256"]
    if expected is not None:
        if not isinstance(expected, str) or not re.fullmatch(r"[a-f0-9]{64}", expected):
            raise ValueError("invalid expected digest")
        try:
            requests = directory / "requests"
            require_private(requests.lstat(), stat.S_ISDIR, 0o700)
            claim = json.loads(read_private(requests, request_id, 2048))
        except FileNotFoundError:
            return request, "dsh_unknown_request"
        value = claim.get("digest")
        claim_cwd = claim.get("cwd")
        if roots and "cwd" not in claim:
            try:
                if read_private(directory, "bridge-session").decode() == session:
                    claim_cwd = read_private(directory, "bridge-workspace").decode()
            except FileNotFoundError:
                return request, "dsh_unknown_request"
        if (claim.get("sessionId") != session or (roots and claim_cwd != cwd)
                or not isinstance(value, str) or not hmac.compare_digest(value, expected)):
            return request, "dsh_request_conflict"
    return request, None


def call():
    directory, session, workspace, method = sys.argv[1:]
    directory = Path(directory)
    require_private(directory.lstat(), stat.S_ISDIR, 0o700)
    path = directory / "bridge.sock"
    require_private(path.lstat(), stat.S_ISSOCK, 0o600)
    payload = read_client_input()
    expected_roots = payload.pop("workspaceScope", None)
    roots = None
    if expected_roots is not None:
        try:
            available = workspace_roots(json.loads(read_private(directory, "bridge-workspaces.json", 32768)))
            roots = workspace_roots(expected_roots)
            if not roots or not covers(available, roots):
                return {"error": "dsh_scope_mismatch"}
        except FileNotFoundError:
            return {"error": "dsh_scope_mismatch"}
    else:
        try:
            (directory / "bridge-workspaces.json").lstat()
        except FileNotFoundError:
            pass
        else:
            return {"error": "dsh_scope_mismatch"}
        if (read_private(directory, "bridge-session").decode() != session
                or read_private(directory, "bridge-workspace").decode() != workspace):
            return {"error": "dsh_scope_mismatch"}
    token = read_private(directory, "bridge-token")
    if not re.fullmatch(rb"[A-Za-z0-9_-]{43}", token):
        raise ValueError("invalid bridge token")
    request, refusal = load_request(method, session, directory, roots, payload)
    if refusal:
        return {"error": refusal}
    if method == "list" and roots:
        # Narrow native access to the requested roots even if the live file is
        # expanded after the local policy check.
        request["workspaceScope"] = expected_roots
    connection = UnixConnection(path, 35 if method == "wait" else 18)
    limit = 524288 if method == "list" and roots else 8192
    try:
        connection.request("POST", "/v1", json.dumps(request, ensure_ascii=False).encode(), {
            "Content-Type": "application/json", "X-DSH-Bridge-Token": token.decode("ascii"),
        })
        with connection.getresponse() as response:
            status = response.status
            body = response.read(limit + 1)
    finally:
        connection.close()
    if len(body) > limit:
        raise ValueError("invalid bridge response")
    payload = json.loads(body)
    if status != 200:
        if payload.get("ok") is not False:
            raise ValueError("invalid bridge refusal")
        error = {
            (400, "invalid"): "dsh_invalid_request",
            (401, "unauthorized"): "dsh_bridge_unauthorized",
            (403, "wrong_session"): "dsh_scope_mismatch",
            (404, "unknown_request"): "dsh_unknown_request",
            (409, "busy"): "dsh_busy",
            (409, "already_claimed"): "submission_uncertain",
            (409, "submission_uncertain"): "submission_uncertain",
            (409, "request_conflict"): "dsh_request_conflict",
            (503, "catalog_changed"): "dsh_catalog_unavailable",
            (503, "catalog_too_large"): "dsh_catalog_unavailable",
        }.get((status, payload.get("error")), "dsh_bridge_unavailable")
        return {"error": error}
    if payload.get("ok") is not True:
        raise ValueError("invalid bridge response")
    value = payload["value"]
    if method == "list":
        return {"value": project_list(value, session, roots)}
    if method == "state":
        return {"value": project_state(value)}
    if method == "events":
        return {"value": project_events(value, request["requestId"])}
    if method == "wait":
        reason = value["reason"]
        if reason not in ("idle", "input-required", "timeout"):
            raise ValueError("invalid bridge wait reason")
        state = project_state(value["state"])
        return {"value": {"reason": reason, "cursor": state["cursor"], "approvalPending": state["approvalPending"]}}
    if value["accepted"] is not True or value["requestId"] != request["requestId"] or not is_count(value["cursor"]):
        raise ValueError("invalid bridge prompt acknowledgement")
    return {"value": {"accepted": True, "requestId": request["requestId"]}}


try:
    result = call()
except (OSError, http.client.HTTPException):
    result = {"error": "dsh_bridge_unavailable"}
except (ValueError, AttributeError, KeyError, TypeError):
    result = {"error": "dsh_bridge_protocol"}
sys.stdout.buffer.write(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")
'''


def _canonical_session_id(value: object) -> str:
    if isinstance(value, str) and value.startswith("session-"):
        suffix = value[8:]
        if str(UUID(suffix)) == suffix:
            return value
    raise ValueError("DSH 会话须使用 session- 开头的完整会话 ID。")


def _absolute_path(value: object) -> PurePosixPath:
    if not isinstance(value, str) or not value.isprintable() or len(value.encode()) > 1024:
        raise ValueError("DSH 路径无效。")
    path = PurePosixPath(value)
    if not path.is_absolute() or not path.name or ".." in path.parts or str(path) != value or value.startswith("//"):
        raise ValueError("DSH 路径须使用规范的绝对路径。")
    return path


_ERRORS = {
    "dsh_bridge_unavailable": "DSH 桥接暂时不可用。",
    "dsh_catalog_unavailable": "DSH 会话目录暂时不可用。",
    "dsh_bridge_unauthorized": "DSH 桥接拒绝了授权。",
    "dsh_bridge_protocol": "DSH 桥接返回了无效数据。",
    "dsh_scope_mismatch": "DSH 会话或工作目录与授权配置不一致。",
    "dsh_busy": "DSH 会话当前正忙或需要处理待办输入。",
    "dsh_invalid_request": "DSH 指令格式无效。",
    "dsh_request_conflict": "DSH 请求与已有记录不一致。",
    "dsh_unknown_request": "DSH 尚无该执行请求的记录。",
    "submission_uncertain": "DSH 指令提交状态不明。",
    "remote_unavailable": "远端执行端暂时不可用。",
}

_DEFINITIVE_SUBMISSION_ERRORS = frozenset({
    "dsh_bridge_unauthorized", "dsh_scope_mismatch", "dsh_busy", "dsh_invalid_request", "dsh_request_conflict",
})


def _failure(code: str) -> PreviewError:
    return PreviewError(code, _ERRORS[code])


@dataclass(frozen=True, slots=True)
class DSHSessionState:
    cursor: int
    active: bool
    running: bool
    queued: int
    pending_input: int
    approval_pending: bool = False


@dataclass(frozen=True, slots=True)
class DSHWorkspace:
    path: PurePosixPath
    include_descendants: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.path, PurePosixPath) or type(self.include_descendants) is not bool:
            raise ValueError("DSH 工作区配置无效。")
        _absolute_path(str(self.path))

    def includes(self, cwd: PurePosixPath) -> bool:
        return cwd == self.path or (self.include_descendants and cwd.is_relative_to(self.path))


@dataclass(frozen=True, slots=True)
class DSHReadTransport:
    ssh: SSHReadTransport | None
    remote_dir: PurePosixPath
    project_root: PurePosixPath | None = None
    session_id: str | None = None
    profile: str = "web"
    workspaces: tuple[DSHWorkspace, ...] = ()
    host: str = "local"

    def __post_init__(self) -> None:
        _absolute_path(str(self.remote_dir))
        if self.workspaces:
            if (
                self.project_root is not None or self.session_id is not None
                or len(self.workspaces) > _WORKSPACE_LIMIT
                or not all(isinstance(scope, DSHWorkspace) for scope in self.workspaces)
                or len({scope.path for scope in self.workspaces}) != len(self.workspaces)
            ):
                raise ValueError("DSH 工作区配置无效。")
        else:
            _canonical_session_id(self.session_id)
            _absolute_path(str(self.project_root))
        if len(str(self.remote_dir / "bridge.sock").encode()) >= 104:
            raise ValueError("DSH 桥接套接字路径过长。")
        if not isinstance(self.profile, str) or not _PROFILE.fullmatch(self.profile):
            raise ValueError("DSH profile 名称无效。")
        if self.ssh is not None and self.ssh.control_path is None:
            raise ValueError("DSH 桥接需要已认证的 SSH 控制连接。")

    @property
    def host(self) -> str:
        return "local" if self.ssh is None else self.ssh.host

    @property
    def can_resume(self) -> bool:
        return bool(self.workspaces)

    def permits(self, session_id: str, cwd: str | PurePosixPath | None) -> bool:
        try:
            _canonical_session_id(session_id)
            path = _absolute_path(str(cwd))
        except ValueError:
            return False
        if self.workspaces:
            return any(scope.includes(path) for scope in self.workspaces)
        return session_id == self.session_id and path == self.project_root

    async def _request(self, method: str, **target: str) -> dict:
        if method not in {"list", "state"}:
            raise ValueError("只支持读取 DSH 会话目录或状态。")
        return await self._bridge_request(method, **target)

    async def watch_completions(self, callback, *, remote_dir: PurePosixPath | None = None):
        """Tail the bridge completion feed. The file is created by the plugin."""
        directory = remote_dir if remote_dir is not None else self.remote_dir
        path = str(directory / "completions.jsonl")
        if self.ssh is None:
            command = [sys.executable, "-u", "-c", _FOLLOW_COMPLETIONS, path]
        else:
            if not await asyncio.to_thread(self.ssh.ensure_control_master):
                raise _failure("remote_unavailable")
            self.ssh._check_control_path()
            command = self.ssh._ssh_command(
                "exec " + shlex.join(("python3", "-u", "-c", _FOLLOW_COMPLETIONS, path))
            )
        process = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, limit=8 * 1024 * 1024,
        )
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                await callback(line.decode("utf-8", "replace"))
        finally:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                process.kill()

    async def _bridge_request(self, method: str, **fields: str | None) -> dict:
        if method not in _METHODS:
            raise ValueError("DSH 桥接操作无效。")
        script_args = (
            "-I", "-c", _BRIDGE_CLIENT, str(self.remote_dir), self.session_id or "",
            str(self.project_root or ""), method,
        )
        if self.ssh is None:
            command = [sys.executable, *script_args]
        else:
            if not await asyncio.to_thread(self.ssh.ensure_control_master):
                raise _failure("remote_unavailable")
            self.ssh._check_control_path()
            command = self.ssh._ssh_command("exec " + shlex.join(("python3", *script_args)))

        payload: dict = dict(fields)
        if self.workspaces:
            payload["workspaceScope"] = {
                "roots": [
                    {"path": str(scope.path), "includeDescendants": scope.include_descendants}
                    for scope in self.workspaces
                ]
            }
        # Projection emits bounded compact UTF-8. Scope is on stdin to keep
        # even large configured root lists out of the remote command.
        response_limit = _LIST_LIMIT if method == "list" and self.workspaces else _RESULT_LIMIT
        environment = {key: os.environ[key] for key in ("HOME", "PATH", "USER", "LOGNAME") if key in os.environ}
        process = None
        try:
            async with asyncio.timeout(40 if method == "wait" else 20):
                process = await asyncio.create_subprocess_exec(
                    *command, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL, env=environment,
                )
                process.stdin.write(json.dumps(payload, ensure_ascii=False).encode())
                await process.stdin.drain()
                process.stdin.close()
                try:
                    await process.stdout.readexactly(response_limit + 1)
                except asyncio.IncompleteReadError as read:
                    response = read.partial
                else:
                    raise _failure("dsh_bridge_protocol")
                if await process.wait() != 0:
                    raise _failure("dsh_bridge_unavailable")
        except (OSError, TimeoutError, UnicodeError):
            raise _failure("dsh_bridge_unavailable") from None
        finally:
            if process is not None and process.returncode is None:
                await _close_server(process)

        try:
            payload = json.loads(response)
            if isinstance(payload, dict) and payload.keys() == {"value"} and isinstance(payload["value"], dict):
                return payload["value"]
            if isinstance(payload, dict) and payload.keys() == {"error"} and payload["error"] in _ERRORS:
                raise _failure(payload["error"])
        except (TypeError, ValueError):
            pass
        raise _failure("dsh_bridge_protocol")

    async def discover(self) -> tuple[SessionCandidate, ...]:
        value = await self._request("list")
        try:
            rows = value["items"]
            if not isinstance(rows, list) or len(rows) > (1024 if self.workspaces else 1):
                raise ValueError
            candidates = {}
            for row in rows:
                title = row["title"]
                session_id = row["sessionId"]
                cwd = row["cwd"] if self.workspaces else self.project_root
                if session_id in candidates or not self.permits(session_id, cwd):
                    raise ValueError
                if title is not None and (not isinstance(title, str) or not title.strip()
                                          or not title.isprintable() or len(title) > 250):
                    title = None
                target = SessionRef(self.host, "dsh", session_id)
                candidates[session_id] = SessionCandidate(target, title, Path(cwd))
            return tuple(candidates.values())
        except (KeyError, TypeError, ValueError):
            raise _failure("dsh_bridge_protocol") from None

    async def state(self, session_id: str, *, cwd: str | PurePosixPath | None = None) -> DSHSessionState:
        target = self._target_fields(session_id, cwd)
        value = await self._request("state", **target)
        counts = ("cursor", "queued", "pendingInput")
        flags = ("active", "running")
        if not (all(type(value.get(key)) is int and value[key] >= 0 for key in counts)
                and all(type(value.get(key)) is bool for key in flags)):
            raise _failure("dsh_bridge_protocol")
        approval_pending = value.get("approvalPending", False)
        if type(approval_pending) is not bool or (approval_pending and not value["pendingInput"]):
            raise _failure("dsh_bridge_protocol")
        return DSHSessionState(
            value["cursor"], value["active"], value["running"], value["queued"], value["pendingInput"], approval_pending,
        )

    async def submit(
        self, session_id: str, text: str, request_id: str, *, cwd: str | PurePosixPath | None = None,
    ) -> None:
        """Send the delivered Markdown once, using the bridge's durable claim."""
        fields = self._target_fields(session_id, cwd)
        self._check_request_id(request_id)
        try:
            valid_text = isinstance(text, str) and text.strip() and len(text.encode()) <= _TEXT_LIMIT
        except UnicodeError:
            valid_text = False
        if not valid_text:
            raise _failure("dsh_invalid_request")

        try:
            value = await self._bridge_request("prompt", **fields, requestId=request_id, text=text)
        except PreviewError as error:
            if error.code in _DEFINITIVE_SUBMISSION_ERRORS:
                raise
            raise _failure("submission_uncertain") from None
        if value != {"accepted": True, "requestId": request_id} or value["accepted"] is not True:
            raise _failure("submission_uncertain")

    async def observe(
        self, session_id: str, request_id: str, *, cwd: str | PurePosixPath | None = None,
        expected_sha256: str | None = None,
    ) -> ExecutionObservation:
        """Reconcile the request using event identifiers and status only."""
        target = self._target_fields(session_id, cwd)
        self._check_request_id(request_id)
        if expected_sha256 is not None and (
            not isinstance(expected_sha256, str) or not _SHA256.fullmatch(expected_sha256)
        ):
            raise _failure("dsh_invalid_request")
        fields = {**target, "requestId": request_id, "expectedSha256": expected_sha256}
        waited = None
        try:
            waited = await self._bridge_request("wait", **fields)
        except PreviewError as error:
            if error.code in {"dsh_unknown_request", "dsh_request_conflict", "dsh_scope_mismatch", "dsh_bridge_unauthorized"}:
                return ExecutionObservation(None, "unknown")
        try:
            value = await self._bridge_request("events", **fields)
        except PreviewError:
            return ExecutionObservation(None, "unknown")
        return _execution_observation(value, request_id, waited)

    def _target_fields(self, session_id: str, cwd: str | PurePosixPath | None) -> dict[str, str]:
        path = self.project_root if cwd is None else cwd
        if not self.permits(session_id, path):
            raise _failure("dsh_scope_mismatch")
        return {"sessionId": session_id, "workspace": str(path)} if self.workspaces else {}

    @staticmethod
    def _check_request_id(request_id: str) -> None:
        try:
            if not isinstance(request_id, str) or str(UUID(request_id)) != request_id:
                raise ValueError
        except ValueError:
            raise _failure("dsh_invalid_request") from None


def _execution_observation(value: dict, request_id: str, waited: dict | None) -> ExecutionObservation:
    unknown = ExecutionObservation(None, "unknown")
    try:
        records = value["records"]
        if not isinstance(records, list) or not records or len(records) > 10:
            return unknown
        user, *rest = records
        if user["type"] != "user/message" or user["requestId"] != request_id:
            return unknown
        turn = user.get("turn")
        previous_seq = user["seq"]
        cursor = value["cursor"]
        output = None
        if (type(previous_seq) is not int or previous_seq < 0 or type(cursor) is not int or previous_seq > cursor
                or (turn is not None and (type(turn) is not int or turn < 0))):
            return unknown
        for index, event in enumerate(rest):
            event_type = event["type"]
            seq, candidate_turn = event["seq"], event["turn"]
            if (type(seq) is not int or not previous_seq < seq <= cursor or type(candidate_turn) is not int or candidate_turn < 0
                    or (turn is not None and turn != candidate_turn)):
                return unknown
            previous_seq = seq
            turn = candidate_turn
            if event_type == "turn/end" and index == len(rest) - 1:
                return ExecutionObservation(
                    str(turn), "completed" if event["reason"] == "completed" else "failed", output,
                )
            if event_type != "assistant/message":
                return unknown
            output = event.get("text") if isinstance(event.get("text"), str) else None
        status = "submitted"
        if (turn is not None and value.get("turnCurrent") is True and waited and waited.get("reason") == "input-required"
                and waited.get("approvalPending") is True and waited.get("cursor") == cursor):
            status = "approval_pending"
        return ExecutionObservation(str(turn) if turn is not None else None, status, output)
    except (KeyError, TypeError):
        return unknown


def _parse_workspaces(rows: object) -> tuple[DSHWorkspace, ...]:
    if not isinstance(rows, list) or not 0 < len(rows) <= _WORKSPACE_LIMIT:
        raise ValueError("DSH workspaces 须为非空工作区列表。")
    workspaces = []
    for row in rows:
        if not isinstance(row, dict) or row.keys() not in ({"path"}, {"path", "include_descendants"}):
            raise ValueError("DSH 工作区配置字段无效。")
        workspaces.append(DSHWorkspace(_absolute_path(row["path"]), row.get("include_descendants", False)))
    return tuple(workspaces)


def parse_dsh_transports(data: object, ssh_scopes: Sequence[SSHScope]) -> tuple[DSHReadTransport, ...]:
    """Bind one scoped DSH profile per local or registered SSH host."""
    if data is None:
        return ()
    if not isinstance(data, dict) or data.keys() != {"hosts"} or not isinstance(data["hosts"], list):
        raise ValueError("DSH 配置须包含 hosts 列表。")
    registered = {scope.transport.host: scope.transport for scope in ssh_scopes}
    transports = {}
    for item in data["hosts"]:
        if not isinstance(item, dict) or item.keys() not in (_LEGACY_KEYS, _WORKSPACE_KEYS):
            raise ValueError("DSH 主机配置字段无效。")
        host = item["host"]
        if not isinstance(host, str) or (host != "local" and host not in registered) or host in transports:
            raise ValueError("DSH 主机须为本机或已登记的 SSH 主机，且只能绑定一个 profile。")
        options = (
            {"workspaces": _parse_workspaces(item["workspaces"])}
            if "workspaces" in item
            else {
                "project_root": _absolute_path(item["project_root"]),
                "session_id": _canonical_session_id(item["session_id"]),
            }
        )
        transports[host] = DSHReadTransport(
            ssh=registered[host] if host != "local" else None,
            remote_dir=_absolute_path(item["bridge_dir"]),
            profile=item["profile"], host=host, **options,
        )
    return tuple(transports.values())
