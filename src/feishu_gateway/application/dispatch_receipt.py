"""Feishu-only dispatch receipt rendering. Never part of the task prompt."""

from __future__ import annotations

from pathlib import PurePosixPath

from feishu_gateway.contracts import SessionRef


def format_dispatch_target(target: SessionRef, cwd: str | PurePosixPath | None, title: str | None) -> str:
    name = (title or "").strip() or "未命名会话"
    workspace = str(cwd) if cwd is not None else "未知"
    return (
        "\n\n**指定会话**\n\n"
        f"- 端侧：{target.host}（{target.runtime}）\n"
        f"- 工作区：`{workspace}`\n"
        f"- 会话：{name}"
    )
