"""Console-side data types that must not require the optional textual extra."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ConsoleSnapshot:
    status: str = "Gateway: unknown"
    feishu: str = "Feishu: unknown"
    active_task: str = "任务：未跟踪"
    hosts: tuple[str, ...] = ()
    workspaces: tuple[str, ...] = ()
    sessions: tuple[str, ...] = ()
    host_commands: tuple[str, ...] = ()
    workspace_commands: tuple[str, ...] = ()
    session_commands: tuple[str, ...] = ()
    unavailable: tuple[str, ...] = ()
    welcome: str = (
        "## 从这里开始\n\n"
        "直接描述任务，或输入命令查看会话、工作区和主机。\n\n"
        "- **自然语言**：`列出所有会话`\n"
        "- **明确投递**：`task prompt HOST WORKSPACE SESSION 任务内容`\n"
        "- **查看结果**：`task result TASK_ID`\n\n"
        "目录条目可预填命令，按 **Enter** 后执行。`exit` 只退出界面，后台继续运行。"
    )
