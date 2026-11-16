"""Pure Markdown presentation for daemon replies; never dispatches operations."""

from __future__ import annotations

import json
import re
import shlex
from dataclasses import dataclass
from typing import Any


_MAX_ROWS = 50
_MAX_OUTPUT = 4000
_STATES = {
    "submitted": "已提交", "running": "执行中", "completed": "已完成",
    "failed": "失败", "uncertain": "提交未确认", "resolved": "已解除", "idle": "空闲",
    "busy": "忙碌", "unknown": "未知", "execution_submitting": "提交未确认",
}


@dataclass(frozen=True, slots=True)
class _Receipt:
    operation: str | None
    data: dict
    messages: tuple[str, ...] = ()
    error: dict | None = None


def _unwrap(result: Any) -> _Receipt:
    operation = None
    messages: list[str] = []
    for _ in range(8):
        if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
            return _Receipt(operation, {}, error={
                "code": "invalid_response", "message": "daemon 返回了无法识别的数据。",
            })
        data = result.get("data")
        data = data if isinstance(data, dict) else {}
        if isinstance(data.get("operation"), str) and isinstance(data.get("result"), dict):
            # Ask envelopes carry the inner Operation result; it is the
            # authoritative outcome even when the outer ok is false (N-20).
            operation = data["operation"]
            result = data["result"]
            continue
        if result["ok"] is not True:
            error = result.get("error")
            return _Receipt(
                operation, data,
                error=error if isinstance(error, dict) else {},
            )
        message = result.get("message")
        if isinstance(message, str) and message.strip() and message not in messages:
            messages.append(message.strip())
        return _Receipt(operation, data, tuple(messages))
    return _Receipt(operation, {}, error={
        "code": "invalid_response", "message": "daemon 返回的数据嵌套过深。",
    })


def _cell(value: Any) -> str:
    """Keep untrusted metadata inside a single Markdown table cell."""
    text = " ".join(str(value if value is not None else "—").split())
    text = "".join(char for char in text if char.isprintable())
    for char in ("\\", "|", "`", "*", "_", "[", "]", "<", ">"):
        text = text.replace(char, "\\" + char)
    return text or "—"


def _code(value: Any, language: str = "text") -> str:
    text = str(value)
    truncated = len(text) > _MAX_OUTPUT
    text = text[:_MAX_OUTPUT]
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    result = f"{fence}{language}\n{text}\n{fence}"
    return result + ("\n\n_内容较长，仅展示前 4000 个字符。_" if truncated else "")


def _table(headers: tuple[str, ...], rows: list[tuple]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    lines.extend("| " + " | ".join(_cell(value) for value in row) + " |" for row in rows[:_MAX_ROWS])
    if len(rows) > _MAX_ROWS:
        lines += ["", f"_仅展示前 {_MAX_ROWS} 项，共 {len(rows)} 项。_"]
    return "\n".join(lines)


def _rows(value: Any) -> list[dict]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, (list, tuple)) else []


def _values(value: Any) -> list:
    return list(value) if isinstance(value, (list, tuple)) else []


def _state(value: Any) -> str:
    return _STATES.get(str(value), str(value or "未知"))


def _error(error: dict, data: dict | None = None) -> str:
    code = str(error.get("code") or "operation_failed")
    if code == "submission_uncertain":
        title = "提交未确认"
        hint = "目标会话可能已经收到任务。请先核对任务状态，避免重复提交。"
    elif code == "confirmation_required":
        title = "需要确认"
        hint = "请使用显式确定性命令确认这项修改。"
    else:
        title = "操作未完成"
        hint = {
            "owner_unavailable": "目标会话的执行连接暂不可用。请先确认会话仍已打开，并查询会话状态。",
            "owner_rejected": "目标会话拒绝外部输入。请检查该会话的外部输入授权与运行状态。",
            "submission_timeout": "投递前检查超时。请先查询目标会话状态和连接情况。",
            "session_busy": "目标会话正在处理其他任务。请先查看任务或会话状态。",
        }.get(code, "")
    parts = [f"### {title}", f"**错误代码** · {_cell(code)}"]
    if error.get("message"):
        parts.append(_cell(error["message"]))
    if hint:
        parts.append(hint)
    data = data if isinstance(data, dict) else {}
    fields = [
        (label, _state(value) if key == "state" else value)
        for label, key in (
            ("任务 ID", "task_id"), ("状态", "state"), ("主机", "host"),
            ("运行时", "runtime"), ("会话 ID", "session_id"), ("工作区", "workspace"),
        )
        if isinstance((value := data.get(key)), str) and value.strip()
    ]
    if fields:
        parts.append(_table(("字段", "内容"), fields))
    task = data.get("task_id")
    if isinstance(task, str) and task.strip() and task.isprintable():
        parts.append("**查询任务状态**\n\n" + _code(shlex.join(("task", "status", task))))
    return "\n\n".join(parts)


def task_id(result: dict) -> str | None:
    """Watch only successful submissions, never nested failures or result queries."""
    receipt = _unwrap(result)
    if receipt.error is not None or receipt.operation not in {None, "task.prompt"}:
        return None
    if receipt.data.get("state") in {
        "completed", "failed", "execution_submitting", "rejected", "duplicate",
    }:
        return None
    value = receipt.data.get("task_id")
    return value if isinstance(value, str) and value else None


def _task_fields(data: dict) -> str:
    target = data.get("target") if isinstance(data.get("target"), dict) else data
    fields = [("状态", _state(data.get("state")))]
    for label, key, source in (
        ("任务 ID", "task_id", data), ("主机", "host", target),
        ("运行时", "runtime", target), ("会话 ID", "session_id", target),
        ("工作区", "workspace", data),
    ):
        if source.get(key):
            fields.append((label, source[key]))
    return _table(("字段", "内容"), fields)


_OBSERVATION_DETAILS = {
    "before_runtime_start": "网关重启后，本进程内已没有该任务的在途观察记录。",
    "lease_held_by_other_task": "同一会话的租约当前由另一个任务持有。",
    "journal_terminal": "执行日志已结束，但结果状态仍未收敛。",
    "lease_expired_or_missing": "该任务已没有有效租约，可能已超出观察窗口。",
}


def _observation_note(data: dict) -> str:
    if data.get("verification_required") is not True:
        return ""
    detail = _OBSERVATION_DETAILS.get(
        str(data.get("observation_reason") or ""),
        "网关无法确认该任务当前的观察状态。",
    )
    return (
        "### 观察中断 · 待核验\n\n" + detail +
        "\n\n这不代表任务未执行，也不代表任务已完成。请在目标会话核对后再决定是否重新发送；"
        "直接重发可能造成重复执行。"
    )


def render_brief(result: dict) -> str:
    receipt = _unwrap(result)
    if receipt.error is not None:
        return "### 任务简报\n\n" + _error(receipt.error, receipt.data)
    data = receipt.data
    parts = ["### 任务简报", _task_fields(data)]
    note = _observation_note(data)
    if note:
        parts.append(note)
    if data.get("brief_source") != "llm":
        parts.append("LLM 摘要不可用。以下为任务状态与原始输出。")
    else:
        if data.get("outcome"):
            parts.append("**结果**\n\n" + _cell(data["outcome"]))
        for label, key in (
            ("关键动作", "key_actions"), ("产出", "artifacts"), ("验证", "verification"),
            ("阻塞", "blockers"), ("后续", "next_steps"),
        ):
            values = data.get(key)
            if isinstance(values, (list, tuple)) and values:
                section = [f"**{label}**", ""] + [f"- {_cell(item)}" for item in values[:4]]
                if len(values) > 4:
                    section.append(f"- 另有 {len(values) - 4} 项。")
                parts.append("\n".join(section))
    if data.get("error"):
        parts.append("**执行信息**\n\n" + _cell(data["error"]))
    output = data.get("output")
    if isinstance(output, str) and output.strip():
        parts.append("**原始输出**\n\n" + _code(output))
    return "\n\n".join(parts)


def render_result(result: dict) -> str:
    receipt = _unwrap(result)
    if receipt.error is not None:
        return _error(receipt.error, receipt.data)
    operation, data = receipt.operation, receipt.data
    if operation in {"session.list", "session.refresh"}:
        rows = _rows(data.get("sessions"))
        body = _table(("会话", "主机 / 类型", "状态"), [
            (row.get("title") or row.get("session_id"),
             f"{row.get('host') or '?'} / {row.get('runtime') or '?'}",
             row.get("state_label") or _state(row.get("state")))
            for row in rows
        ]) if rows else "当前没有可用会话。可先检查已登记的工作区，或刷新目录。"
        rendered = f"### 会话（{len(rows)}）\n\n{body}"
    elif operation == "session.status":
        rendered = "### 会话状态\n\n" + _table(("字段", "内容"), [
            ("名称", data.get("title") or data.get("session_id")),
            ("状态", data.get("state_label") or _state(data.get("state"))),
            ("主机", data.get("host")), ("运行时", data.get("runtime")),
            ("会话 ID", data.get("session_id")), ("工作区", data.get("cwd") or data.get("workspace")),
        ])
    elif operation == "host.candidates":
        rows = _rows(data.get("hosts"))
        body = _table(("候选主机", "地址", "用户", "端口"), [
            (row.get("alias"), row.get("hostname"), row.get("user"), row.get("port"))
            for row in rows
        ]) if rows else "当前没有可添加的候选主机。"
        rendered = f"### 候选主机（{len(rows)}）\n\n{body}"
    elif operation == "host.list":
        rows = _rows(data.get("hosts"))
        body = _table(("主机", "别名", "已登记工作区"), [
            (row.get("host") or row.get("alias"),
             "、".join(str(value) for value in _values(row.get("aliases"))) or "—",
             len(_values(row.get("registered_workspaces")))) for row in rows
        ]) if rows else "当前没有主机记录。"
        rendered = f"### 主机（{len(rows)}）\n\n{body}"
    elif operation == "workspace.list":
        rows = [(row.get("host"), path) for row in _rows(data.get("hosts"))
                for path in _values(row.get("workspaces")) if isinstance(path, str)]
        if data.get("status") == "host_not_configured":
            body = f"主机 {_cell(data.get('host'))} 尚未配置。"
        else:
            body = _table(("主机", "工作区"), rows) if rows else "当前没有已登记的工作区。"
        rendered = f"### 工作区（{len(rows)}）\n\n{body}"
    elif operation == "task.prompt":
        state = data.get("state")
        if state in {"uncertain", "execution_submitting"}:
            return _error({"code": "submission_uncertain"}, data)
        if state in {"failed", "rejected", "duplicate"}:
            return _error({"code": str(state), "message": data.get("error") or "任务未成功提交。"}, data)
        if not isinstance(data.get("task_id"), str) or not data["task_id"]:
            return _error({"code": "invalid_response", "message": "提交回执缺少任务 ID，请先核对任务状态。"})
        heading = "任务已完成" if state == "completed" else "任务已提交"
        rendered = f"### {heading}\n\n" + _task_fields(data)
    elif operation == "task.status":
        rendered = "### 任务状态\n\n" + _task_fields(data)
        note = _observation_note(data)
        if note:
            rendered += "\n\n" + note
        if data.get("error"):
            rendered += "\n\n**执行信息**\n\n" + _cell(data["error"])
    elif operation == "task.result":
        return render_brief(result)
    else:
        rendered = "### 操作完成" if operation else "### 结果"
        if data:
            rendered += "\n\n" + _code(json.dumps(data, ensure_ascii=False, indent=2, default=str), "json")
    if receipt.messages:
        rendered += "\n\n" + "\n\n".join(_cell(message) for message in receipt.messages)
    return rendered
