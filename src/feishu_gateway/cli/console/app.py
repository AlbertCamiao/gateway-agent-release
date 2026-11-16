"""Textual console shell: fixed status, catalog sidebar, Markdown, input."""

from __future__ import annotations

import asyncio
import json
import shlex
from dataclasses import dataclass
from typing import Protocol

from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.suggester import SuggestFromList
from textual.widgets import Input, Label, ListItem, ListView, Markdown, Static

from feishu_gateway.application.dispatch_receipt import format_dispatch_target
from feishu_gateway.cli.console.types import ConsoleSnapshot
from feishu_gateway.execution.task_runtime import format_task_prompt
from feishu_gateway.contracts import SessionRef
from feishu_gateway.application.presentation import render_brief, render_result, task_id as _task_id


class ConsoleClient(Protocol):
    async def snapshot(self) -> ConsoleSnapshot: ...
    async def ask(self, text: str) -> dict: ...
    async def rpc(self, operation: str, args: dict, *, confirmed: bool = False) -> dict: ...


class NullConsoleClient:
    async def snapshot(self) -> ConsoleSnapshot:
        return ConsoleSnapshot()

    async def ask(self, text: str) -> dict:
        return {"ok": False, "error": {"code": "daemon_unavailable", "message": "daemon unavailable"}}

    async def rpc(self, operation: str, args: dict, *, confirmed: bool = False) -> dict:
        return {"ok": False, "error": {"code": "daemon_unavailable", "message": "daemon unavailable"}}


_COMMANDS = {
    "task", "session", "workspace", "host", "dsh", "connect", "doctor", "completion", "op",
}


class CatalogItem(ListItem):
    """Display a catalog row and an optional explicit, unsubmitted command."""

    def __init__(self, row: str, command: str = "") -> None:
        parts = row.split(" · ")
        label = Text(parts[1] if len(parts) > 2 else parts[0], style="bold #dce7f5")
        if len(parts) > 2:
            label.append("\n" + parts[0] + " · " + " · ".join(parts[2:]), style="#91a4bc")
        elif len(parts) == 2:
            label.append("\n" + parts[1], style="#91a4bc")
        super().__init__(Label(label))
        self.command = command
        self.tooltip = row + ("\n预填命令，按 Enter 执行" if command else "")


class ConsoleApp(App):
    TITLE = "Gateway Console"
    SUB_TITLE = "Sessions · Tasks · Results"
    CSS = """
    Screen { layout: vertical; background: #0c1421; color: #dce7f5; }
    #status {
        dock: top;
        height: 3;
        padding: 0 2;
        background: #142033;
    }
    #body { height: 1fr; layout: horizontal; padding: 1 1 0 1; }
    #sidebar {
        width: 32;
        min-width: 24;
        padding: 0 1;
        margin-right: 1;
        background: #101b2b;
        border: round #263950;
        border-title-color: #91a4bc;
    }
    .section-title { height: 1; text-style: bold; color: #67d9cc; margin-top: 1; }
    #hosts { height: auto; max-height: 5; min-height: 1; }
    #workspaces { height: auto; max-height: 6; min-height: 1; }
    #sessions { height: 1fr; min-height: 2; }
    ListView { background: transparent; scrollbar-size-vertical: 1; }
    ListItem { height: auto; min-height: 1; padding: 0 1; margin-bottom: 1; }
    ListItem Label { width: 1fr; height: auto; }
    ListView > ListItem.--highlight { background: #234153; }
    ListView:focus > ListItem.--highlight { background: #295363; }
    .empty-row { color: #8092ab; }
    #workspace-pane { width: 1fr; }
    #markdown-scroll {
        width: 1fr;
        height: 1fr;
        border: round #263950;
        border-title-color: #91a4bc;
        background: #101b2b;
        overflow-y: auto;
        scrollbar-size-vertical: 1;
        scrollbar-color: #3a6478;
        scrollbar-color-hover: #67d9cc;
    }
    #markdown { width: 1fr; padding: 1 2; background: transparent; }
    MarkdownH1, MarkdownH2, MarkdownH3 { color: #8ce2d8; background: transparent; }
    MarkdownBlockQuote { border-left: thick #4ba99f; background: #15273a; }
    MarkdownTable { margin: 1 0; }
    MarkdownTableContent > .header { color: #8ce2d8; text-style: bold; }
    #input {
        dock: bottom;
        height: 3;
        margin: 0 1 1 1;
        padding: 0 1;
        background: #142237;
        border: round #3c566e;
    }
    #input:focus { border: round #67d9cc; }
    #input-hint { dock: bottom; height: 1; padding: 0 2; color: #91a4bc; }
    Screen.compact #sidebar { display: none; }
    Screen.sidebar-hidden #sidebar { display: none; }
    Screen.compact.sidebar-open #sidebar { display: block; width: 26; }
    Screen.compact #body { padding-top: 0; }
    Screen.compact #markdown { padding: 0 1; }
    Screen.compact #status { padding: 0 1; }
    Screen.short .section-title { margin-top: 0; }
    Screen.short #body { padding-top: 0; }
    Screen.short #hosts, Screen.short #workspaces { max-height: 3; }
    Screen.short ListItem { margin-bottom: 0; }
    """

    BINDINGS = [
        ("ctrl+q", "quit", "Quit"),
        ("ctrl+r", "refresh", "Refresh"),
        ("ctrl+l", "clear", "Clear"),
        ("ctrl+b", "toggle_sidebar", "Catalog"),
        ("f1", "help", "Help"),
        ("escape", "focus_input", "Input"),
        ("ctrl+up", "scroll_markdown(-8)", "Scroll up"),
        ("ctrl+down", "scroll_markdown(8)", "Scroll down"),
        ("ctrl+home", "transcript_start", "First result"),
        ("ctrl+end", "transcript_end", "Latest result"),
    ]

    def __init__(self, client: ConsoleClient | None = None) -> None:
        super().__init__()
        self.client = client or NullConsoleClient()
        self.last_submission = ""
        self.snapshot = ConsoleSnapshot()
        self._lines: list[str] = []
        self._history: list[str] = []
        self._history_index = 0
        self._history_draft = ""
        self._busy = False
        self._refreshing = False
        self._active_tasks: dict[str, str] = {}
        self._noted_verification: set[str] = set()
        self._notice = "就绪"

    def compose(self) -> ComposeResult:
        yield Static(self.snapshot.status, id="status", markup=False)
        yield Static("Enter 发送  ·  ↑↓ 历史  ·  → 补全  ·  F1 帮助  ·  Esc 输入", id="input-hint", markup=False)
        yield Input(
            placeholder="描述任务，或输入命令…",
            suggester=SuggestFromList([
                "session list", "workspace list", "host list", "host candidates",
                "task status ", "task result ", "task resolve ",
                "doctor", "help", "clear", "exit",
            ], case_sensitive=False),
            id="input",
        )
        with Horizontal(id="body"):
            with Vertical(id="sidebar"):
                yield Static("主机", id="hosts-title", classes="section-title")
                yield ListView(id="hosts")
                yield Static("工作区", id="workspaces-title", classes="section-title")
                yield ListView(id="workspaces")
                yield Static("会话", id="sessions-title", classes="section-title")
                yield ListView(id="sessions")
            with Vertical(id="workspace-pane"):
                with VerticalScroll(id="markdown-scroll"):
                    yield Markdown(self.snapshot.welcome, id="markdown")

    def on_mount(self) -> None:
        self._responsive_layout(self.size.width)
        self.query_one("#markdown-scroll").border_title = "对话与结果"
        self.query_one("#sidebar").border_title = "目录"
        self.query_one("#input", Input).focus()
        self._refreshing = True
        self.run_worker(self._refresh_catalog(), group="catalog-refresh", exclusive=False)

    async def _reload(self) -> None:
        try:
            self.snapshot = await self.client.snapshot()
        except Exception:
            self.snapshot = ConsoleSnapshot(
                status="Gateway: unavailable", unavailable=("hosts", "workspaces", "sessions"),
            )
        self._set_status()
        self._fill("#hosts", self.snapshot.hosts, self.snapshot.host_commands)
        self._fill("#workspaces", self.snapshot.workspaces, self.snapshot.workspace_commands)
        self._fill("#sessions", self.snapshot.sessions, self.snapshot.session_commands)
        self.query_one("#markdown", Markdown).update(
            self.snapshot.welcome + ("\n\n" + "\n\n".join(self._lines) if self._lines else "")
        )
        self.query_one("#input", Input).focus()

    def _set_status(self) -> None:
        running = "running" in self.snapshot.status
        status = Text("◈ GATEWAY  ", style="bold #8ce2d8")
        status.append("CONSOLE", style="#91a4bc")
        status.append("   ● " + self.snapshot.status, style="#86d8ac" if running else "#f4bf75")
        status.append("\n" + self.snapshot.feishu, style="#a8b9d0")
        counts = " · ".join(
            f"{'—' if name in self.snapshot.unavailable else len(rows)} {label}"
            for name, label, rows in (
                ("hosts", "主机", self.snapshot.hosts),
                ("workspaces", "工作区", self.snapshot.workspaces),
                ("sessions", "会话", self.snapshot.sessions),
            )
        )
        status.append("   " + counts, style="#91a4bc")
        activity = "working… 正在发送" if self._busy else self._notice
        if self._active_tasks:
            activity += f" · 跟踪 {len(self._active_tasks)} 个任务"
        status.append("\n" + activity + "  ·  Ctrl+R 刷新 · Ctrl+B 目录 · Ctrl+L 清屏 · Ctrl+Q 退出", style="#91a4bc")
        self.query_one("#status", Static).update(status)

    def _fill(self, selector: str, rows: tuple[str, ...], commands: tuple[str, ...] = ()) -> None:
        view = self.query_one(selector, ListView)
        view.clear()
        unavailable = selector[1:] in self.snapshot.unavailable
        self.query_one(selector + "-title", Static).update(
            {"#hosts": "主机", "#workspaces": "工作区", "#sessions": "会话"}[selector]
            + ("  —" if unavailable else f"  {len(rows):02d}")
        )
        if not rows:
            label = "暂不可用 · Ctrl+R 重试" if unavailable else "暂无条目"
            view.append(ListItem(Label(label, classes="empty-row"), disabled=True))
            return
        for index, row in enumerate(rows):
            view.append(CatalogItem(row, commands[index] if index < len(commands) else ""))

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, CatalogItem) and event.item.command:
            field = self.query_one("#input", Input)
            if field.value.strip():
                self._notice = "已有输入草稿；请先发送或清空，再选择目录"
            else:
                field.value = event.item.command
                field.cursor_position = len(field.value)
                self._notice = "已预填命令 · 按 Enter 执行"
            field.focus()
            self._set_status()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        if self._busy:
            self._notice = "当前请求仍在等待；输入草稿已保留"
            self.query_one("#input-hint", Static).update(self._notice)
            return
        event.input.value = ""
        if text:
            self.run_worker(self.submit(text), group="submit", exclusive=False)

    async def submit(self, text: str) -> None:
        text = text.strip()
        if not text:
            return
        if self._busy and text not in {"help", "clear", "exit", "quit"}:
            self._notice = "当前请求仍在等待；完成后可继续操作"
            self.query_one("#input-hint", Static).update(self._notice)
            return
        self.last_submission = text
        self._history.append(text)
        self._history_index = len(self._history)
        self._history_draft = ""
        if text in {"exit", "quit"}:
            self.exit()
            return
        if text == "clear":
            self.action_clear()
            return
        if text == "help":
            self._append("## 命令与操作\n\n"
                         "直接输入自然语言，或使用 `ask TEXT`。目录选择只预填命令，按 Enter 后执行。\n\n"
                         "| 用途 | 命令 |\n| --- | --- |\n"
                         "| 查看目录 | `session list` · `workspace list` · `host list` |\n"
                         "| 会话状态 | `session status HOST SESSION` |\n"
                         "| 投递任务 | `task prompt HOST WORKSPACE SESSION TEXT` |\n"
                         "| 查看任务 | `task status ID` · `task result ID` |\n"
                         "| 解除待核验 | `task resolve ID [REASON]`（网关侧解除，不终止远端） |\n"
                         "| 工作区管理 | `workspace register HOST PATH` · `workspace remove HOST PATH` |\n"
                         "| 连接与诊断 | `connect HOST` · `dsh status HOST` · `doctor [HOST] [WORKSPACE]` |\n"
                         "| 其他操作 | `host candidates` · `completion sync HOST` · `op NAME JSON [--confirmed]` |\n\n"
                         "**快捷键**：Ctrl+R 刷新目录；Ctrl+B 显示/隐藏目录；Ctrl+L 清屏；"
                         "Ctrl+↑/↓ 滚动；Ctrl+Home/End 首条/最新；↑↓ 历史；Esc 回到输入；Ctrl+Q 退出。\n\n"
                         "显式修改命令会执行对应修改；自然语言修改请求仍需改用明确命令确认。"
                         "`exit` 只退出 console，后台继续运行。")
            return
        self._append("---\n\n**你**\n\n" + "\n".join("> " + line for line in text.splitlines()))
        self._busy = True
        self._set_status()
        try:
            result = await self._execute(text)
            preview = await self._resolved_preview(result)
            if preview:
                self._append(preview)
            self._append(render_result(result))
            task_id = _task_id(result)
            if task_id:
                self.run_worker(
                    self._watch_task(task_id), group=f"task-{task_id}", exclusive=False,
                )
        except Exception as error:
            self._append(f"**console error**\n\n`{type(error).__name__}: {error}`")
        finally:
            self._busy = False
            self._notice = "就绪"
            self._set_status()
            self.query_one("#input-hint", Static).update("Enter 发送  ·  ↑↓ 历史  ·  → 补全  ·  F1 帮助  ·  Esc 输入")
            self.query_one("#input", Input).focus()

    async def _task_preview(self, text: str) -> str:
        try:
            parts = shlex.split(text)
        except ValueError:
            return ""
        if len(parts) < 5 or parts[:2] != ["task", "prompt"]:
            return ""
        host, workspace, session = parts[2], parts[3], parts[4]
        runtime = "?"
        title = None
        try:
            state = await self.client.rpc("session.status", {"host": host, "session": session})
            if state.get("ok") and isinstance(state.get("data"), dict):
                runtime = str(state["data"].get("runtime") or runtime)
                title = state["data"].get("title")
        except Exception:
            pass
        return _preview_for_args({
            "host": host, "runtime": runtime, "workspace": workspace,
            "session": session, "title": title, "text": " ".join(parts[5:]),
        })

    async def _resolved_preview(self, result: dict) -> str:
        if not isinstance(result, dict) or not result.get("ok"):
            return ""
        data = result.get("data")
        if not isinstance(data, dict) or data.get("operation") != "task.prompt":
            return ""
        args = data.get("args")
        if not isinstance(args, dict):
            return ""
        args = dict(args)
        nested = data.get("result") if isinstance(data.get("result"), dict) else {}
        if nested.get("ok") is not True:
            return ""
        payload = nested.get("data") if isinstance(nested.get("data"), dict) else {}
        prompt_markdown = payload.get("prompt_markdown")
        if not args.get("runtime") or not args.get("title"):
            host = args.get("host")
            session = args.get("session")
            if isinstance(host, str) and isinstance(session, str):
                try:
                    state = await self.client.rpc(
                        "session.status", {"host": host, "session": session},
                    )
                    if state.get("ok") and isinstance(state.get("data"), dict):
                        args.setdefault("runtime", state["data"].get("runtime"))
                        args.setdefault("title", state["data"].get("title"))
                except Exception:
                    pass
        return _preview_for_args(args, prompt_markdown=prompt_markdown)

    async def _watch_task(self, task_id: str) -> None:
        self._active_tasks[task_id] = "running"
        self._set_status()
        try:
            for _ in range(150):
                try:
                    result = await self.client.rpc("task.status", {"task_id": task_id})
                except Exception:
                    self._append(f"**状态查询中断**\n\n可使用 `task status {task_id}` 再次查询；任务状态未因此改变。")
                    return
                if not result.get("ok"):
                    self._append(render_result(result))
                    return
                data = result.get("data") if isinstance(result.get("data"), dict) else {}
                state = str(data.get("state") or "")
                self._active_tasks[task_id] = state
                if (
                    data.get("verification_required") is True
                    and state not in {"completed", "failed", "uncertain"}
                    and task_id not in self._noted_verification
                ):
                    self._noted_verification.add(task_id)
                    self._append(
                        "**观察中断 · 待核验**\n\n"
                        "网关已没有该任务的在途观察记录；这不代表任务未执行或已完成。"
                        "可继续用 `task status` 查询，重发前先核对。"
                    )
                if state in {"completed", "failed", "uncertain"}:
                    try:
                        brief = await self.client.rpc("task.result", {"task_id": task_id})
                        self._append(render_brief(brief))
                    except Exception:
                        self._append(f"**结果暂不可用**\n\n请使用 `task result {task_id}` 查看结果。")
                    return
                await asyncio.sleep(2)
            self._append(f"**已暂停自动查询**\n\n可使用 `task status {task_id}` 继续查看，任务状态未因此改变。")
        finally:
            self._active_tasks.pop(task_id, None)
            self._set_status()

    async def _execute(self, text: str) -> dict:
        parts = shlex.split(text)
        if not parts:
            return {"ok": False, "error": {"code": "invalid_args", "message": "empty command"}}
        if parts[0] == "ask":
            return await self.client.ask(" ".join(parts[1:]))
        if parts[0] not in _COMMANDS:
            return await self.client.ask(text)
        return await self._run_command(parts)

    async def _rpc(self, operation: str, args: dict, *, confirmed: bool = False) -> dict:
        result = await self.client.rpc(operation, args, confirmed=confirmed)
        if result.get("ok") and isinstance(result.get("data"), dict):
            return {"ok": True, "data": {
                "operation": operation, "args": dict(args), "result": result,
            }}
        return result

    async def _run_command(self, parts: list[str]) -> dict:
        command = parts[0]
        if command == "task":
            if len(parts) >= 5 and parts[1] == "prompt":
                return await self._rpc("task.prompt", {
                    "host": parts[2], "workspace": parts[3], "session": parts[4],
                    "text": " ".join(parts[5:]),
                })
            if len(parts) == 3 and parts[1] in {"status", "result"}:
                return await self._rpc(f"task.{parts[1]}", {"task_id": parts[2]})
            if len(parts) >= 3 and parts[1] == "resolve":
                args = {"task_id": parts[2]}
                if len(parts) > 3:
                    args["reason"] = " ".join(parts[3:])
                return await self._rpc("task.resolve", args, confirmed=True)
        elif command == "session":
            if len(parts) >= 2 and parts[1] == "list":
                return await self._rpc("session.list", {"host": parts[2]} if len(parts) > 2 else {})
            if len(parts) == 4 and parts[1] == "status":
                return await self._rpc("session.status", {"host": parts[2], "session": parts[3]})
            if len(parts) >= 3 and parts[1] == "refresh":
                args = {"host": parts[2]}
                if len(parts) > 3:
                    args["workspace"] = parts[3]
                return await self._rpc("session.refresh", args)
        elif command == "workspace":
            if len(parts) >= 2 and parts[1] == "list":
                return await self._rpc("workspace.list", {"host": parts[2]} if len(parts) > 2 else {})
            if len(parts) == 4 and parts[1] in {"register", "remove"}:
                return await self._rpc(
                    f"workspace.{parts[1]}", {"host": parts[2], "workspace": parts[3]}, confirmed=True,
                )
        elif command == "host":
            if len(parts) == 2 and parts[1] in {"list", "candidates"}:
                return await self._rpc(f"host.{parts[1]}", {})
            if len(parts) == 3 and parts[1] == "remove":
                return await self._rpc("host.remove", {"host": parts[2]}, confirmed=True)
        elif command == "dsh" and len(parts) >= 3:
            args = {"host": parts[2]}
            if "--profile" in parts:
                index = parts.index("--profile")
                if index + 1 < len(parts):
                    args["profile"] = parts[index + 1]
            return await self._rpc(f"dsh.{parts[1]}", args, confirmed=parts[1] == "connect")
        elif command == "connect" and len(parts) >= 2:
            args = {"host": parts[1], "no_dsh": "--no-dsh" in parts, "no_codex": "--no-codex" in parts}
            if "--profile" in parts:
                index = parts.index("--profile")
                if index + 1 < len(parts):
                    args["profile"] = parts[index + 1]
            return await self._rpc("connect", args, confirmed=True)
        elif command == "doctor":
            return await self._rpc("doctor", {
                "host": parts[1] if len(parts) > 1 else None,
                "workspace": parts[2] if len(parts) > 2 else None,
            })
        elif command == "completion" and len(parts) >= 3 and parts[1] == "sync":
            return await self._rpc(
                "completion.sync", {"host": parts[2], "dsh": "--no-dsh" not in parts}, confirmed=True,
            )
        elif command == "op" and len(parts) >= 2:
            confirmed = "--confirmed" in parts
            clean = [part for part in parts if part != "--confirmed"]
            args = json.loads(clean[2]) if len(clean) > 2 else {}
            if not isinstance(args, dict):
                raise ValueError("op args must be a JSON object")
            return await self._rpc(clean[1], args, confirmed=confirmed)
        return {"ok": False, "error": {"code": "invalid_args", "message": f"incomplete command: {command}"}}

    def _append(self, text: str) -> None:
        pane = self.query_one("#markdown-scroll", VerticalScroll)
        follow_latest = pane.is_vertical_scroll_end
        self._lines.append(text)
        self._lines = self._lines[-200:]
        self.query_one("#markdown", Markdown).update(
            self.snapshot.welcome + "\n\n" + "\n\n".join(self._lines)
        )
        if follow_latest:
            self.call_after_refresh(pane.scroll_end, animate=False)

    def on_mouse_scroll_down(self, event: events.MouseScrollDown) -> None:
        if self._in_transcript(event.widget):
            self.action_scroll_markdown(4)
            event.stop()
            event.prevent_default()

    def on_mouse_scroll_up(self, event: events.MouseScrollUp) -> None:
        if self._in_transcript(event.widget):
            self.action_scroll_markdown(-4)
            event.stop()
            event.prevent_default()

    @staticmethod
    def _in_transcript(widget) -> bool:
        while widget is not None:
            if widget.id == "markdown-scroll":
                return True
            widget = widget.parent
        return False

    def on_resize(self, event: events.Resize) -> None:
        self._responsive_layout(event.size.width, event.size.height)

    def _responsive_layout(self, width: int, height: int | None = None) -> None:
        compact = width < 90
        if compact != self.screen.has_class("compact"):
            self.screen.remove_class("sidebar-hidden", "sidebar-open")
        self.screen.set_class(compact, "compact")
        self.screen.set_class((height if height is not None else self.size.height) < 30, "short")

    def action_toggle_sidebar(self) -> None:
        if self.screen.has_class("compact"):
            self.screen.toggle_class("sidebar-open")
        else:
            self.screen.toggle_class("sidebar-hidden")

    async def action_help(self) -> None:
        await self.submit("help")

    def action_transcript_start(self) -> None:
        self.query_one("#markdown-scroll", VerticalScroll).scroll_home(animate=False)

    def action_transcript_end(self) -> None:
        self.query_one("#markdown-scroll", VerticalScroll).scroll_end(animate=False)

    async def on_key(self, event: events.Key) -> None:
        if event.key not in {"up", "down"}:
            return
        if not self.query_one("#input", Input).has_focus:
            return
        if not self._history:
            return
        event.prevent_default()
        event.stop()
        field = self.query_one("#input", Input)
        if event.key == "up":
            if self._history_index == len(self._history):
                self._history_draft = field.value
            self._history_index = max(0, self._history_index - 1)
        else:
            self._history_index = min(len(self._history), self._history_index + 1)
        field.value = self._history[self._history_index] if self._history_index < len(self._history) else self._history_draft
        field.cursor_position = len(field.value)

    def action_clear(self) -> None:
        self._lines.clear()
        self.query_one("#markdown", Markdown).update(self.snapshot.welcome)

    def action_focus_input(self) -> None:
        self.query_one("#input", Input).focus()

    def action_scroll_markdown(self, amount: int) -> None:
        self.query_one("#markdown-scroll", VerticalScroll).scroll_relative(y=amount, animate=False)

    async def action_refresh(self) -> None:
        if not self._refreshing:
            self._refreshing = True
            self.run_worker(self._refresh_catalog(), group="catalog-refresh", exclusive=False)

    async def _refresh_catalog(self) -> None:
        self._notice = "正在刷新目录…"
        self._set_status()
        try:
            await self._reload()
        finally:
            self._refreshing = False
            self._notice = "就绪"
            self._set_status()


def _preview_from_result(result: dict) -> str:
    if not isinstance(result, dict) or not result.get("ok"):
        return ""
    data = result.get("data")
    if not isinstance(data, dict) or data.get("operation") != "task.prompt":
        return ""
    args = data.get("args")
    return _preview_for_args(args) if isinstance(args, dict) else ""


def _preview_for_args(args: dict, *, prompt_markdown=None) -> str:
    host = args.get("host")
    workspace = args.get("workspace")
    session = args.get("session")
    text = args.get("text")
    if not all(isinstance(value, str) and value for value in (host, workspace, session, text)):
        return ""
    runtime = str(args.get("runtime") or "?")
    title = args.get("title")
    target = format_dispatch_target(
        SessionRef(host, runtime, session), workspace, title or session,
    ).strip()
    prompt = prompt_markdown if isinstance(prompt_markdown, str) and prompt_markdown.strip() else format_task_prompt(text)
    return target + "\n\n" + prompt
