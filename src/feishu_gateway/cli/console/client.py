"""Daemon-backed data source for the console shell."""

from __future__ import annotations

import asyncio
import shlex
from typing import Any

from feishu_gateway.cli.console.types import ConsoleSnapshot


class ConsoleDaemonClient:
    def __init__(self, paths=None, *, client=None) -> None:
        self._paths = paths
        self._client = client

    def _transport(self):
        if self._client is not None:
            return self._client
        from feishu_gateway.daemon import DaemonClient, DaemonPaths

        self._paths = self._paths or DaemonPaths.from_home()
        self._client = DaemonClient(self._paths)
        return self._client

    async def ask(self, text: str) -> dict:
        return await asyncio.to_thread(
            self._transport().ask, text, origin="console", timeout=300.0,
        )

    async def rpc(self, operation: str, args: dict, *, confirmed: bool = False) -> dict:
        return await asyncio.to_thread(
            self._transport().rpc, operation, args, origin="console", confirmed=confirmed,
            timeout=180.0,
        )

    async def snapshot(self) -> ConsoleSnapshot:
        client = self._transport()
        try:
            health = await asyncio.to_thread(client.health)
        except Exception:
            return ConsoleSnapshot(
                status="Gateway: unavailable",
                feishu="Feishu: unknown",
                active_task="任务：未跟踪",
                unavailable=("hosts", "workspaces", "sessions"),
                welcome=("# Gateway 控制台\n\nDaemon unavailable — 后台服务暂不可用。\n\n"
                         "在另一个终端运行 `gateway-agent start`，然后按 `Ctrl+R` 重试。"),
            )
        try:
            ingress = await asyncio.to_thread(client.ingress_status)
        except Exception:
            ingress = {}

        results = await asyncio.gather(
            asyncio.to_thread(client.rpc, "host.list"),
            asyncio.to_thread(client.rpc, "workspace.list"),
            asyncio.to_thread(client.rpc, "session.list"),
            return_exceptions=True,
        )
        host_rows = _rows(results[0], "hosts")
        workspace_rows = _rows(results[1], "hosts")
        session_rows = _rows(results[2], "sessions")

        host_entries = [row for row in host_rows if isinstance(row, dict) and row.get("host")]
        workspace_entries = [
            (row, path)
            for row in workspace_rows if isinstance(row, dict)
            if isinstance(row.get("workspaces"), (list, tuple))
            for path in row["workspaces"]
        ]
        session_entries = [row for row in session_rows if isinstance(row, dict)]
        hosts = tuple(str(row["host"]) for row in host_entries)
        workspaces = tuple(f"{row.get('host')}: {path}" for row, path in workspace_entries)
        sessions = tuple(_session_line(row) for row in session_entries)
        host_commands = tuple(_command(("session", "list"), row.get("host")) for row in host_entries)
        workspace_commands = tuple(
            _command(("doctor",), row.get("host"), path)
            for row, path in workspace_entries
        )
        session_commands = tuple(
            _command(("session", "status"), row.get("host"), row.get("session_id"))
            for row in session_entries
        )

        pid = health.get("pid", "?")
        enabled = bool(ingress.get("enabled"))
        connected = bool(ingress.get("connected"))
        bound = bool(ingress.get("bound"))
        if not enabled:
            feishu = "Feishu: disabled"
        elif connected:
            feishu = "Feishu: connected" + (" · bound" if bound else "")
        else:
            feishu = "Feishu: disconnected"

        failed = [
            name for name, key, result in zip(
                ("主机", "工作区", "会话"), ("hosts", "hosts", "sessions"), results,
            )
            if not _rows_available(result, key)
        ]
        notice = f"\n\n**暂不可用：{'、'.join(failed)}。** 按 `Ctrl+R` 重试。" if failed else ""
        return ConsoleSnapshot(
            status=f"Gateway: running (PID {pid})",
            feishu=feishu,
            active_task="任务：未跟踪",
            hosts=hosts,
            workspaces=workspaces,
            sessions=sessions,
            host_commands=host_commands,
            workspace_commands=workspace_commands,
            session_commands=session_commands,
            unavailable=tuple(
                name for name, key, result in zip(
                    ("hosts", "workspaces", "sessions"), ("hosts", "hosts", "sessions"), results,
                ) if not _rows_available(result, key)
            ),
            welcome=(
                "# Gateway 控制台\n\n"
                "直接输入任务描述，或使用确定性命令。\n\n"
                "- **浏览目录**：点击左侧条目，预填只读查询；按 `Enter` 执行。\n"
                "- **查看会话**：输入 `session list`；**查看工作区**：输入 `workspace list`。\n"
                "- **查看帮助**：输入 `help`；**退出界面**：输入 `exit`，后台服务继续运行。"
            ) + notice,
        )


def _command(prefix: tuple[str, ...], *values: object) -> str:
    if not all(isinstance(value, str) and value.strip() and value.isprintable() for value in values):
        return ""
    return shlex.join((*prefix, *values))


def _rows_available(result: Any, key: str) -> bool:
    return (
        isinstance(result, dict) and bool(result.get("ok"))
        and isinstance(result.get("data"), dict)
        and isinstance(result["data"].get(key), list)
    )


def _rows(result: Any, key: str) -> list:
    if not isinstance(result, dict) or not result.get("ok"):
        return []
    data = result.get("data")
    if not isinstance(data, dict):
        return []
    rows = data.get(key)
    return list(rows) if isinstance(rows, list) else []


def _session_line(row: dict) -> str:
    title = row.get("title") or str(row.get("session_id") or "untitled")[:8]
    runtime = row.get("runtime") or "?"
    state = row.get("state_label") or row.get("state") or "?"
    return f"{row.get('host')}/{runtime} · {title} · {state}"


def run(paths=None) -> int:
    from feishu_gateway.cli.console.app import ConsoleApp

    app = ConsoleApp(ConsoleDaemonClient(paths))
    app.run()
    return 0
