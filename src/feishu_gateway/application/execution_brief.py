"""Feishu rendering for a completed task summary. No execution logic."""

from __future__ import annotations

from feishu_gateway.contracts import ExecutionBrief


def format_execution_brief(brief: ExecutionBrief) -> str:
    title = brief.target_title if brief.target_title else "未命名会话"
    lines = [
        "**指定会话**：" + title,
        "",
        "**任务简报**",
        "",
        f"- 状态：{brief.state}",
    ]
    if brief.outcome:
        lines.append(f"- 结果：{brief.outcome}")
    for label, values in (
        ("关键动作", brief.key_actions),
        ("产出", brief.artifacts),
        ("验证", brief.verification),
        ("阻塞", brief.blockers),
        ("后续", brief.next_steps),
    ):
        if values:
            lines.append(f"- {label}：")
            lines.extend(f"  - {item}" for item in values[:4])
    return "\n".join(lines)
