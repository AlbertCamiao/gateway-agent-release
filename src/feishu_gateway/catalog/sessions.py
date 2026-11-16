"""Discover routable Codex threads within one authorized local project."""

import asyncio
from pathlib import Path
from uuid import UUID

from feishu_gateway.executors.codex_app_server import PreviewError, _AppServer, _close_server
from feishu_gateway.catalog.types import INTERACTIVE_SOURCES, SessionCandidate
from feishu_gateway.contracts import SessionRef


def _candidate(thread: dict, project_root: Path) -> SessionCandidate | None:
    if (
        thread.get("source") not in INTERACTIVE_SOURCES
        or thread.get("ephemeral") is not False
        or thread.get("parentThreadId") is not None
    ):
        return None

    cwd = thread.get("cwd")
    if not isinstance(cwd, str) or not Path(cwd).is_absolute():
        return None
    try:
        if Path(cwd).resolve(strict=True) != project_root:
            return None
    except (OSError, RuntimeError, ValueError):
        return None

    thread_id = thread.get("id")
    if not isinstance(thread_id, str):
        return None
    try:
        if str(UUID(thread_id)) != thread_id:
            return None
    except ValueError:
        return None

    title = thread.get("name")
    if not isinstance(title, str) or not title.strip() or not title.isprintable():
        title = None
    return SessionCandidate(SessionRef("local", "codex", thread_id), title, project_root)


async def _list_sessions(app: _AppServer, project_root: Path) -> tuple[SessionCandidate, ...]:
    await app.call(
        "initialize", {"clientInfo": {"name": "feishu_gateway_catalog", "version": "0.1"}}
    )
    await app.send({"method": "initialized"})

    params = {
        "cwd": str(project_root),
        "archived": False,
        "limit": 50,
        "sourceKinds": INTERACTIVE_SOURCES,
        "useStateDbOnly": True,
    }
    candidates = {}
    seen_cursors = set()
    while True:
        page = await app.call("thread/list", params)
        for thread in page["data"]:
            candidate = _candidate(thread, project_root)
            if candidate is not None:
                candidates.setdefault(candidate.target.session_id, candidate)

        cursor = page.get("nextCursor")
        if cursor is None:
            return tuple(candidates.values())
        if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
            raise PreviewError("catalog_protocol", "Codex 会话目录返回了无效的分页游标。")
        seen_cursors.add(cursor)
        params["cursor"] = cursor


async def discover_sessions(
    *, codex_command: str, project_root: Path
) -> tuple[SessionCandidate, ...]:
    """Read scoped thread metadata and return only fields used by routing."""
    try:
        project_root = project_root.resolve(strict=True)
        if not project_root.is_dir():
            raise NotADirectoryError
    except (OSError, RuntimeError, ValueError):
        raise PreviewError("catalog_scope", "会话发现目录不可用。") from None

    process = None
    try:
        async with asyncio.timeout(30):
            process = await asyncio.create_subprocess_exec(
                codex_command, "app-server", "--listen", "stdio://",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=2**20,
            )
            return await _list_sessions(_AppServer(process), project_root)
    except (OSError, TimeoutError):
        raise PreviewError("catalog_unavailable", "本机 Codex 会话目录暂不可用。") from None
    except (KeyError, TypeError, AttributeError, ValueError):
        raise PreviewError("catalog_protocol", "Codex 会话目录返回了无效的数据。") from None
    finally:
        if process is not None:
            await _close_server(process)
