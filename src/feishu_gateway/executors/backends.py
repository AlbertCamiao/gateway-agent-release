"""Codex/DSH backend descriptions and read-only session status."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath

from feishu_gateway.catalog.types import RoutedInstruction
from feishu_gateway.executors.codex_app_server import PreviewError
from feishu_gateway.executors.codex_local import read_owner_state
from feishu_gateway.executors.codex_remote import is_workspace_cwd
from feishu_gateway.executors.dsh import DSHReadTransport


OWNER_REPLIES = {
    "owner_unavailable": "已找到目标 Session，但暂时无法联系它的执行端。请在对应主机的 Codex 面板打开该会话。",
    "owner_state_unknown": "已连接目标 Session 的执行端，但暂未收到会话状态。请确认该会话已在 Codex 面板中打开。",
    "owner_rejected": "已找到目标 Session，但当前的 Codex 执行端不接受外部指令。",
    "session_unavailable": "已找到目标 Session，但它尚未打开、正在忙或工作目录已变化。",
    "remote_unavailable": "已找到目标 Session，但暂时无法核对远端工作目录，指令未提交。",
    "execution_policy_unavailable": "已找到目标 Session，但暂时无法核对它的执行权限。",
}


@dataclass(frozen=True, slots=True)
class CodexBackend:
    project_root: Path
    codex_command: str | None
    server_factory: Callable[[], AbstractAsyncContextManager] | None = None
    owner_socket: Path | None = None
    writable_sessions: frozenset[str] | None = None
    check_cwd: Callable[[PurePosixPath], Awaitable[None]] | None = None
    ensure_owner: Callable[[], Awaitable[None]] | None = None
    repair_owner: Callable[[], Awaitable[None]] | None = None
    transport: object | None = None

    def allows_write(self, thread_id: str) -> bool:
        return self.writable_sessions is None or thread_id in self.writable_sessions

    def owner_options(self) -> dict:
        return {
            "expected_cwd": self.project_root,
            "socket_path": self.owner_socket,
            "remote_paths": self.server_factory is not None,
        }

    def reader_options(self) -> dict:
        return {"codex_command": self.codex_command, "server_factory": self.server_factory}


def codex_backend_for(
    cwd: str | None, exact: tuple[CodexBackend, ...], workspaces: tuple[CodexBackend, ...],
) -> CodexBackend | None:
    if cwd is None:
        return exact[0] if len(exact) == 1 else None
    for backend in exact:
        if str(backend.project_root) == cwd:
            return backend
    for backend in workspaces:
        if is_workspace_cwd(cwd, (PurePosixPath(backend.project_root),)):
            return replace(backend, project_root=Path(cwd))
    return None


async def dsh_session_status(
    routed: RoutedInstruction, transport: DSHReadTransport | None,
) -> str:
    target = routed.target
    if transport is None or not transport.permits(target.session_id, routed.cwd):
        return "scope_mismatch"
    try:
        state = await transport.state(target.session_id, cwd=routed.cwd)
    except PreviewError as error:
        return "scope_mismatch" if error.code == "dsh_scope_mismatch" else "unknown"
    if state.running or state.queued or state.pending_input:
        return "busy"
    if not state.active:
        return "resumable" if transport.can_resume else "unattached"
    return "idle"


async def codex_session_status(
    routed: RoutedInstruction, backend: CodexBackend,
) -> str:
    target = routed.target
    if not backend.allows_write(target.session_id):
        return "read_only"
    if backend.server_factory is not None and backend.owner_socket is None:
        return "owner_not_configured"

    async def read_state():
        if backend.ensure_owner is not None:
            await backend.ensure_owner()
        if backend.check_cwd is not None:
            await backend.check_cwd(PurePosixPath(backend.project_root))
        return await read_owner_state(target.session_id, **backend.owner_options())

    try:
        async with asyncio.timeout(10):
            try:
                state = await read_state()
            except PreviewError as error:
                if error.code != "owner_unavailable" or backend.repair_owner is None:
                    raise
                await backend.repair_owner()
                state = await read_state()
    except PreviewError as error:
        if error.code in OWNER_REPLIES or error.code in {"remote_unavailable", "session_unavailable"}:
            return error.code
        return "unknown"
    except (OSError, TimeoutError, ValueError):
        return "unknown"
    runtime = state.get("threadRuntimeStatus")
    status = runtime.get("type") if isinstance(runtime, dict) else None
    return {"idle": "idle", "active": "busy"}.get(status, "owner_state_unknown")
