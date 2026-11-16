"""R1.3 task runtime: dispatch, lease, completion, result store."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Mapping
from uuid import uuid4

from feishu_gateway.discovery import scan_session_rows
from feishu_gateway.feishu.replies import validate_prompt_markdown
from feishu_gateway.execution.arbiter import SessionArbiter, SessionBusy
from feishu_gateway.execution.completion_source import RuntimeCompletionSource
from feishu_gateway.execution.delivery_store import DeliveryStore
from feishu_gateway.execution.observation import describe_observation
from feishu_gateway.execution.result_store import ResultStore
from feishu_gateway.home import GatewayHome
from feishu_gateway.operation_dispatch import OperationResult
from feishu_gateway.operations import Operation
from feishu_gateway.execution.journal import ExecutionJournal, JournalConflict
from feishu_gateway.contracts import CompletionEvent, SessionRef, TaskDescriptor
from feishu_gateway.contracts import (
    DeliveryContext, ExecutionBrief, ExecutionEvidence, PortError,
)
from feishu_gateway.workspaces.registry import read_registry


LOGGER = logging.getLogger("feishu-gateway")

_TASK_TRANSITIONS = {
    "new": {"submitted", "rejected", "uncertain"},
    "submitted": {"running", "completed", "failed", "rejected", "uncertain"},
    "running": {"completed", "failed"},
    "uncertain": {"completed", "failed", "resolved"},
    "completed": set(),
    "failed": set(),
    "rejected": set(),
    "resolved": set(),
}


def _can_transition(current: str, target: str) -> bool:
    return current == target or target in _TASK_TRANSITIONS.get(current, set())


@dataclass(slots=True)
class PreparedTask:
    operation: Operation
    target: SessionRef
    task_id: str
    host: str
    runtime: str
    session: str
    workspace: str
    cwd: PurePosixPath
    row: Mapping[str, Any]
    text: str
    formatted: str
    delivery: DeliveryContext
    fail: Callable[..., Awaitable[OperationResult]]

    def to_payload(self) -> dict[str, Any]:
        return {
            "operation": {
                "request_id": self.operation.request_id,
                "name": self.operation.name,
                "args": dict(self.operation.args),
                "origin": self.operation.origin,
                "confirmed": self.operation.confirmed,
                "delivery": dict(self.operation.delivery) if self.operation.delivery else None,
            },
            "target": {
                "host": self.target.host,
                "runtime": self.target.runtime,
                "session_id": self.target.session_id,
            },
            "task_id": self.task_id,
            "host": self.host,
            "runtime": self.runtime,
            "session": self.session,
            "workspace": self.workspace,
            "cwd": str(self.cwd),
            "row": dict(self.row),
            "text": self.text,
            "formatted": self.formatted,
            "delivery": {
                "chat_id": self.delivery.chat_id,
                "sender_id": self.delivery.sender_id,
                "message_id": self.delivery.message_id,
            },
        }

    @classmethod
    def from_payload(
        cls, payload: Mapping[str, Any], fail: Callable[..., Awaitable[OperationResult]],
    ) -> "PreparedTask":
        raw_operation = payload["operation"]
        operation = Operation(
            request_id=str(raw_operation["request_id"]),
            name=str(raw_operation["name"]),
            args=dict(raw_operation.get("args") or {}),
            origin=str(raw_operation.get("origin") or "cli"),
            confirmed=bool(raw_operation.get("confirmed")),
            delivery=dict(raw_operation["delivery"]) if raw_operation.get("delivery") else None,
        )
        target = payload["target"]
        delivery = payload["delivery"]
        return cls(
            operation=operation,
            target=SessionRef(str(target["host"]), str(target["runtime"]), str(target["session_id"])),
            task_id=str(payload["task_id"]),
            host=str(payload["host"]),
            runtime=str(payload["runtime"]),
            session=str(payload["session"]),
            workspace=str(payload["workspace"]),
            cwd=PurePosixPath(str(payload["cwd"])),
            row=dict(payload["row"]),
            text=str(payload["text"]),
            formatted=str(payload["formatted"]),
            delivery=DeliveryContext(
                str(delivery["chat_id"]), str(delivery["sender_id"]), str(delivery["message_id"]),
            ),
            fail=fail,
        )


SessionLookup = Callable[[str, str | None, str], Awaitable[dict[str, Any] | None]]
SessionProbe = Callable[
    [str, str | None, str], Awaitable[tuple[dict[str, Any] | None, int]],
]


def format_task_prompt(text: str) -> str:
    return (
        "## 任务目标\n\n"
        f"{text.strip()}\n\n"
        "## 输入与约束\n\n"
        "- 目标会话已由 Gateway 根据本次请求明确选择。\n"
        "- 只执行本次任务，不扩大授权范围。\n\n"
        "## 执行步骤\n\n"
        "1. 阅读任务目标。\n"
        "2. 在已选会话中完成目标。\n\n"
        "## 验收标准\n\n"
        "- 返回完成结果或明确失败原因。\n"
    )


class TaskRuntime:
    def __init__(
        self, home: GatewayHome | None = None, *,
        execution_port=None,
        registry=None,
        result_store: ResultStore | None = None, arbiter: SessionArbiter | None = None,
        journal: ExecutionJournal | None = None, session_lookup: SessionLookup | None = None,
        session_probe: SessionProbe | None = None,
        completion: bool = True,
        summarize=None, result_sink=None, prompt_formatter=None,
        lease_ttl: float | None = None, heartbeat_interval: float | None = None,
        heartbeat_stale_after: float | None = None,
        delivery_store: DeliveryStore | None = None,
        recovery_window: float = 3600.0,
        observation_window_seconds: float = 86400.0,
    ) -> None:
        self.home = home or GatewayHome.discover()
        self._execution_port = execution_port
        self._registry = dict(registry or {})
        self._result_store = result_store or ResultStore(self.home.daemon_state_path)
        self._arbiter = arbiter or SessionArbiter(
            self.home.daemon_state_path,
            **({"default_ttl": float(lease_ttl)} if lease_ttl is not None else {}),
        )
        self._heartbeat_interval = (
            float(heartbeat_interval) if heartbeat_interval is not None
            else self._arbiter.default_ttl / 3.0
        )
        self._heartbeats: dict[str, asyncio.Task] = {}
        self._heartbeat_stale_after = (
            float(heartbeat_stale_after) if heartbeat_stale_after is not None
            else max(60.0, self._heartbeat_interval * 2.0)
        )
        self._recovered: set[str] = set()
        self._seen_completion_events: set[str] = set()
        self._recovery_window = float(recovery_window)
        self._observation_window_seconds = float(observation_window_seconds)
        self._journal = journal
        self._session_lookup = session_lookup
        self._session_probe = session_probe
        self._completion_enabled = completion
        self._summarize = summarize
        self._result_sink = result_sink
        self._memory_sink = None
        self._completion_sink = None
        self._completion_source: RuntimeCompletionSource | None = None
        self._completion_events_task: asyncio.Task | None = None
        self._inbox_task: asyncio.Task | None = None
        self._inbox_wakeup = asyncio.Event()
        self._delivery_store = delivery_store
        self._prompt_formatter = prompt_formatter
        self._started = False
        self._started_at: float | None = None
        self._start_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            await self._start_once()

    async def _start_once(self) -> None:
        self._started_at = time.time()
        # Create the additive delivery/outbox schema before the journal bumps
        # the shared schema version, so the migration is atomic and backup-safe.
        await asyncio.to_thread(self._deliveries)
        if self._journal is None:
            self._journal = ExecutionJournal(self.home.daemon_state_path)
        if self._execution_port is None:
            raise RuntimeError("execution port is not configured")
        if self._completion_source is None:
            if self._execution_port is None:
                raise RuntimeError("execution port is not configured")
            self._completion_source = self._execution_port.completion_source()
        self._completion_events_task = asyncio.create_task(
            self._consume_completion_events(),
        )
        await self._reconcile_pending()
        if self._completion_enabled:
            self._inbox_task = asyncio.create_task(self._consume_completion_inbox())
        self._started = True

    @staticmethod
    def _format_prompt(text: str) -> str:
        return format_task_prompt(text)

    def set_prompt_formatter(self, formatter) -> None:
        self._prompt_formatter = formatter

    def set_summarizer(self, summarize) -> None:
        self._summarize = summarize

    def set_result_sink(self, sink) -> None:
        self._result_sink = sink

    def set_memory_sink(self, sink) -> None:
        self._memory_sink = sink

    async def _reconcile_pending(self) -> None:
        """Reattach observation to in-flight tasks without re-dispatching.

        Only tasks that still show a valid lease or were updated within the
        recovery window are treated as in-flight. Older leftovers stay
        display-only (user decision a) and are handled by explicit resolution.
        """
        if self._journal is None:
            return
        now = time.time()
        entries = await asyncio.to_thread(self._journal.pending_execution)
        active_stages = {
            "execution_submitting", "execution_queued",
            "execution_started", "awaiting_approval",
        }
        recovered = 0
        recovered_rows: list[Mapping[str, Any]] = []
        for entry in entries:
            if entry.stage not in active_stages:
                continue
            row = await asyncio.to_thread(self._result_store.get, entry.message_id)
            if row is None or str(row.get("state") or "") not in {
                "submitted", "running", "uncertain",
            }:
                continue
            if not await self._recovery_eligible(row, now):
                continue
            self._recovered.add(entry.message_id)
            recovered_rows.append(row)
            recovered += 1
        for row in await asyncio.to_thread(self._result_store.unfinished_tasks):
            task_id = str(row.get("task_id") or "")
            if not task_id or task_id in self._recovered:
                continue
            if not await self._recovery_eligible(row, now):
                continue
            self._recovered.add(task_id)
            recovered_rows.append(row)
            recovered += 1
        for row in recovered_rows:
            await self._renew_recovered_lease(row, now)
        if recovered:
            LOGGER.info("Recovered %d in-flight task(s) at startup.", recovered)

    async def _renew_recovered_lease(self, row: Mapping[str, Any], now: float) -> None:
        task_id = str(row.get("task_id") or "")
        target = SessionRef(
            str(row.get("host") or "local"), str(row.get("runtime") or "codex"),
            str(row.get("session_id") or ""),
        )
        if not target.session_id:
            return
        try:
            lease = await asyncio.to_thread(self._arbiter.lease, target)
            owner = str((lease or {}).get("task_id") or "")
            expires = float((lease or {}).get("expires_at") or 0)
            if owner != task_id or expires <= now:
                await asyncio.to_thread(
                    self._arbiter.acquire, target, task_id, "daemon",
                )
            self._start_heartbeat(target, task_id)
        except SessionBusy:
            LOGGER.warning("Recovered task lease is held by another task; guard remains.")
        except Exception:
            LOGGER.warning("Recovered task lease could not be renewed.")

    async def _recovery_eligible(self, row: Mapping[str, Any], now: float) -> bool:
        host = str(row.get("host") or "")
        runtime = str(row.get("runtime") or "")
        session = str(row.get("session_id") or "")
        task_id = str(row.get("task_id") or "")
        if host and runtime and session and task_id:
            lease = await asyncio.to_thread(
                self._arbiter.lease, SessionRef(host, runtime, session),
            )
            if lease is not None and str(lease.get("task_id") or "") == task_id:
                expires = lease.get("expires_at")
                if isinstance(expires, (int, float)) and not isinstance(expires, bool):
                    if float(expires) > now:
                        return True
        updated = row.get("updated_at")
        if isinstance(updated, (int, float)) and not isinstance(updated, bool):
            return 0 <= now - float(updated) <= self._recovery_window
        return False

    def _deliveries(self) -> DeliveryStore:
        if self._delivery_store is None:
            self._delivery_store = DeliveryStore(self.home.daemon_state_path)
        return self._delivery_store

    async def _on_completion(
        self, evidence: ExecutionEvidence, delivery: DeliveryContext, prompt: str,
        *, event_id: str,
    ) -> None:
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("completion event_id is required")
        if self._completion_sink is None:
            raise RuntimeError("completion sink is not configured")
        target = evidence.target
        await self._completion_sink(
            evidence.task_id,
            {
                "event_id": event_id,
                "evidence": {
                    "task_id": evidence.task_id,
                    "target": {
                        "host": target.host, "runtime": target.runtime,
                        "session_id": target.session_id,
                    },
                    "state": evidence.state,
                    "task_summary": evidence.task_summary,
                    "output": evidence.output,
                    "error": evidence.error,
                },
                "delivery": {
                    "chat_id": delivery.chat_id,
                    "sender_id": delivery.sender_id,
                    "message_id": delivery.message_id,
                },
                "prompt": prompt,
            },
        )

    async def complete_from_event(self, event: dict) -> bool:
        session_id = event.get("session_id")
        turn_id = event.get("turn_id")
        state = event.get("state")
        event_id = event.get("event_id") or event.get("eventId")
        if (
            not isinstance(session_id, str)
            or not isinstance(turn_id, str)
            or not isinstance(event_id, str)
            or not event_id
        ):
            return False
        if self._journal is None or state not in {"completed", "failed"}:
            return False
        entry = await asyncio.to_thread(
            self._journal.find_by_target_execution_ref,
            str(event.get("host") or "local"),
            str(event.get("runtime") or ""),
            session_id, turn_id,
        )
        if entry is None:
            return False
        event_host = event.get("host")
        if (
            isinstance(event_host, str) and event_host
            and str(entry.host or "local").lower() != event_host.lower()
        ):
            return False
        event_runtime = event.get("runtime")
        if (
            isinstance(event_runtime, str) and event_runtime
            and str(entry.runtime or "").lower() != event_runtime.lower()
        ):
            return False
        chat_id = (
            await asyncio.to_thread(self._journal.delivery_chat, entry.message_id)
        ) or "oc_cli"
        output = event.get("output")
        prompt = entry.prompt or "已提交的任务"
        evidence = ExecutionEvidence(
            entry.message_id,
            SessionRef(entry.host or "local", entry.runtime, entry.thread_id or ""),
            state, prompt, output=output if isinstance(output, str) else None,
        )
        await self._on_completion(
            evidence, DeliveryContext(chat_id, "cli", entry.message_id),
            prompt, event_id=event_id,
        )
        if entry.stage in {"execution_submitting", "execution_queued"}:
            entry = await asyncio.to_thread(
                self._journal.advance,
                entry.message_id, "execution_started", execution_turn_id=turn_id,
            )
        try:
            await asyncio.to_thread(self._journal.advance, entry.message_id, state)
        except JournalConflict:
            LOGGER.warning("Completion fact was recorded but the journal state is stale.")
        return True

    async def dbos_completion_persist(self, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        raw = payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else {}
        target = raw.get("target") if isinstance(raw.get("target"), Mapping) else {}
        evidence = ExecutionEvidence(
            str(raw.get("task_id") or ""),
            SessionRef(str(target.get("host") or ""), str(target.get("runtime") or ""), str(target.get("session_id") or "")),
            str(raw.get("state") or "unknown"), str(raw.get("task_summary") or ""),
            output=raw.get("output") if isinstance(raw.get("output"), str) else None,
            error=raw.get("error") if isinstance(raw.get("error"), str) else None,
        )
        self._recovered.discard(evidence.task_id)
        if evidence.state not in {"completed", "failed"}:
            raise ValueError("completion state must be completed or failed")
        event_id = str(payload.get("event_id") or "")
        if not event_id:
            raise ValueError("completion event_id is required")
        claim = await asyncio.to_thread(
            self._result_store.claim_completion_event, event_id, evidence.task_id,
        )
        if claim == "completed":
            return None
        if claim == "conflict":
            raise ValueError("completion event is bound to another task")
        prompt = str(payload.get("prompt") or "")
        existing = await asyncio.to_thread(self._result_store.get, evidence.task_id) or {}
        terminal = str(existing.get("state") or "") in {
            "completed", "failed", "rejected", "resolved",
        }
        if terminal and claim != "pending":
            await asyncio.to_thread(
                self._result_store.complete_completion_event, event_id,
            )
            return None
        origin = str(existing.get("origin") or "cli")
        brief: ExecutionBrief | None = None
        if self._summarize is not None:
            try:
                brief = await self._summarize(evidence, prompt)
            except Exception:
                LOGGER.warning("execution brief summarizer failed")
        if brief is None:
            output = evidence.output if isinstance(evidence.output, str) else ""
            brief = ExecutionBrief(
                task_id=evidence.task_id, target=evidence.target, state=evidence.state,
                outcome=(output.strip().splitlines()[0][:500] if output.strip() else evidence.state),
                blockers=(evidence.error,) if evidence.error else (),
            )
        if not terminal:
            await self._transition(
                evidence.task_id, evidence.state, origin=origin,
                host=evidence.target.host, runtime=evidence.target.runtime,
                session_id=evidence.target.session_id, outcome=brief.outcome,
                output=evidence.output, error=evidence.error,
            )
        self._stop_heartbeat(evidence.task_id)
        stored_delivery = None
        if self._result_sink is not None:
            try:
                deliveries = await asyncio.to_thread(self._deliveries)
                stored_delivery = await asyncio.to_thread(deliveries.delivery, evidence.task_id)
            except Exception:
                LOGGER.warning("Task delivery identity could not be read")
        chosen = stored_delivery if isinstance(stored_delivery, Mapping) else None
        return {
            "task_id": evidence.task_id,
            "event_id": event_id,
            "origin": origin,
            "brief": asdict(brief),
            "evidence": {
                "task_id": evidence.task_id,
                "target": {
                    "host": evidence.target.host, "runtime": evidence.target.runtime,
                    "session_id": evidence.target.session_id,
                },
                "state": evidence.state, "task_summary": evidence.task_summary,
                "output": evidence.output, "error": evidence.error,
            },
            "delivery": chosen,
            "prompt": prompt,
        }

    async def dbos_completion_release(self, payload: Mapping[str, Any]) -> None:
        raw = payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else {}
        target = raw.get("target") if isinstance(raw.get("target"), Mapping) else {}
        await asyncio.to_thread(
            self._arbiter.release,
            SessionRef(str(target.get("host") or ""), str(target.get("runtime") or ""), str(target.get("session_id") or "")),
            str(payload.get("task_id") or ""),
        )

    async def dbos_completion_receipt(self, payload: Mapping[str, Any]) -> None:
        event_id = str(payload.get("event_id") or "")
        if not event_id:
            raise ValueError("completion event_id is required")
        raw = payload.get("brief") if isinstance(payload.get("brief"), Mapping) else {}
        target_raw = raw.get("target") if isinstance(raw.get("target"), Mapping) else {}
        brief = ExecutionBrief(
            str(raw.get("task_id") or ""),
            SessionRef(str(target_raw.get("host") or ""), str(target_raw.get("runtime") or ""), str(target_raw.get("session_id") or "")),
            str(raw.get("state") or "unknown"), str(raw.get("outcome") or ""),
            tuple(raw.get("key_actions") or ()), tuple(raw.get("artifacts") or ()),
            tuple(raw.get("verification") or ()), tuple(raw.get("blockers") or ()),
            tuple(raw.get("next_steps") or ()),
        )
        delivery_raw = payload.get("delivery") if isinstance(payload.get("delivery"), Mapping) else None
        delivery = None
        if isinstance(delivery_raw, Mapping) and delivery_raw.get("chat_id"):
            delivery = DeliveryContext(
                str(delivery_raw.get("chat_id") or ""), str(delivery_raw.get("sender_id") or ""),
                str(delivery_raw.get("message_id") or ""),
            )
        if delivery is not None and self._result_sink is not None:
            await self._result_sink(brief, {
                "chat_id": delivery.chat_id, "sender_id": delivery.sender_id,
                "message_id": delivery.message_id,
            })
        if self._memory_sink is not None:
            raw_evidence = payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else {}
            target2 = raw_evidence.get("target") if isinstance(raw_evidence.get("target"), Mapping) else {}
            evidence = ExecutionEvidence(
                str(raw_evidence.get("task_id") or ""),
                SessionRef(str(target2.get("host") or ""), str(target2.get("runtime") or ""), str(target2.get("session_id") or "")),
                str(raw_evidence.get("state") or "unknown"), str(raw_evidence.get("task_summary") or ""),
                output=raw_evidence.get("output") if isinstance(raw_evidence.get("output"), str) else None,
                error=raw_evidence.get("error") if isinstance(raw_evidence.get("error"), str) else None,
            )
            await self._memory_sink(
                str(payload.get("origin") or "cli"),
                {
                    "chat_id": delivery.chat_id, "sender_id": delivery.sender_id,
                    "message_id": delivery.message_id,
                } if delivery is not None else None,
                evidence, brief, str(payload.get("prompt") or ""),
            )
        await asyncio.to_thread(
            self._result_store.complete_completion_event, event_id,
        )

    async def _dsh_recovery_candidates(self):
        rows = await asyncio.to_thread(self._result_store.unfinished_tasks)
        candidates = []
        for row in rows:
            if str(row.get("runtime") or "") != "dsh":
                continue
            task_id = str(row.get("task_id") or "")
            session_id = str(row.get("session_id") or "")
            workspace = str(row.get("workspace") or "")
            if not task_id or not session_id or not workspace or self._journal is None:
                continue
            entry = await asyncio.to_thread(self._journal.get, task_id)
            if entry is None or not entry.execution_request_id:
                continue
            candidates.append({
                "target": SessionRef(
                    str(row.get("host") or "local"), "dsh", session_id,
                ),
                "execution_ref": entry.execution_request_id,
                "session_id": session_id,
                "workspace": workspace,
                "updated_at": row.get("updated_at"),
            })
        return tuple(candidates)

    async def codex_recovery_candidates(self):
        rows = await asyncio.to_thread(self._result_store.unfinished_tasks)
        candidates = []
        for row in rows:
            if str(row.get("runtime") or "") != "codex":
                continue
            task_id = str(row.get("task_id") or "")
            session_id = str(row.get("session_id") or "")
            workspace = str(row.get("workspace") or "")
            if not task_id or not session_id or not workspace or self._journal is None:
                continue
            entry = await asyncio.to_thread(self._journal.get, task_id)
            if entry is None or not entry.execution_turn_id:
                continue
            candidates.append({
                "target": SessionRef(
                    str(row.get("host") or "local"), "codex", session_id,
                ),
                "execution_ref": entry.execution_turn_id,
                "workspace": workspace,
                "updated_at": row.get("updated_at"),
            })
        return tuple(candidates)

    async def _persist_completion_event(self, event: CompletionEvent) -> None:
        journal = self._journal
        if journal is None:
            raise RuntimeError("completion journal is not configured")
        await asyncio.to_thread(
            journal.record_inbox_event,
            event.event_id, "completion-source", event.state,
            host=event.target.host, runtime=event.target.runtime,
            session_id=event.target.session_id,
            turn_id=event.execution_ref,
            output=event.output,
        )
        self._inbox_wakeup.set()

    async def _consume_completion_inbox(self) -> None:
        journal = self._journal
        if journal is None:
            return
        while True:
            pending = await asyncio.to_thread(journal.pending_inbox, 16)
            if not pending:
                try:
                    await asyncio.wait_for(self._inbox_wakeup.wait(), timeout=5.0)
                except TimeoutError:
                    pass
                self._inbox_wakeup.clear()
                continue
            retry_pending = False
            for raw in pending:
                event_id = raw.get("event_id")
                if not isinstance(event_id, str):
                    continue
                if raw.get("state") not in {"completed", "failed"}:
                    await asyncio.to_thread(journal.consume_inbox, event_id)
                    continue
                try:
                    handled = await self.complete_from_event({
                        "event_id": event_id,
                        "host": raw.get("host"),
                        "runtime": raw.get("runtime"),
                        "session_id": raw.get("session_id") or "",
                        "turn_id": raw.get("turn_id") or "",
                        "state": raw.get("state"),
                        "output": raw.get("output"),
                    })
                except Exception:
                    attempts = await asyncio.to_thread(
                        journal.mark_inbox_attempt, event_id, "handler_failed",
                    )
                    LOGGER.exception("Completion inbox event failed: %s", event_id)
                    if attempts >= 30:
                        LOGGER.error(
                            "Completion inbox event permanently failed: %s", event_id,
                        )
                        await asyncio.to_thread(journal.consume_inbox, event_id)
                    else:
                        retry_pending = True
                    continue
                if handled:
                    await asyncio.to_thread(journal.consume_inbox, event_id)
                else:
                    attempts = await asyncio.to_thread(
                        journal.mark_inbox_attempt, event_id, "unmatched",
                    )
                    if attempts >= 30:
                        LOGGER.warning(
                            "Completion inbox event exceeded correlation window: %s",
                            event_id,
                        )
                        await asyncio.to_thread(journal.consume_inbox, event_id)
                    else:
                        retry_pending = True
            if retry_pending:
                await asyncio.sleep(0.5)

    async def _consume_completion_events(self) -> None:
        source = self._completion_source
        if source is None:
            return
        seen_events = getattr(self, "_seen_completion_events", None)
        if seen_events is None:
            seen_events = self._seen_completion_events = set()
        async for event in source.events():
            if event.event_id in seen_events:
                continue
            payload = {
                "event_id": event.event_id,
                "host": event.target.host,
                "runtime": event.target.runtime,
                "session_id": event.target.session_id,
                "turn_id": event.execution_ref,
                "state": event.state,
                "output": event.output,
            }
            for attempt in range(10):
                try:
                    handled = await self.complete_from_event(payload)
                except Exception:
                    LOGGER.exception("Completion event could not be handled: %s", event.event_id)
                    break
                if handled:
                    seen_events.add(event.event_id)
                    if len(seen_events) > 4096:
                        seen_events.clear()
                    break
                # The Codex watcher can observe completion before the dispatch
                # step persists the journal correlation. Give the writer a
                # short window instead of dropping the fact.
                if attempt == 9:
                    LOGGER.warning("Normalized completion event did not match a task: %s", event.event_id)
                else:
                    await asyncio.sleep(0.1)

    async def close(self) -> None:
        pending = [task for task in self._heartbeats.values() if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._heartbeats.clear()
        for task_name in ("_inbox_task", "_completion_events_task"):
            task = getattr(self, task_name)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                setattr(self, task_name, None)
        if self._completion_source is not None:
            await self._completion_source.close()
            self._completion_source = None
        self._recovered.clear()
        self._started = False

    async def _lookup(self, host: str, workspace: str | None, session: str) -> dict[str, Any] | None:
        if self._session_lookup is not None:
            return await self._session_lookup(host, workspace, session)
        registry = await asyncio.to_thread(read_registry, self.home.registry_path)
        rows, _ = await scan_session_rows(
            host, PurePosixPath(workspace) if workspace is not None else None, registry=registry,
        )
        return next((row for row in rows if row.get("session_id") == session), None)

    def set_completion_sink(self, sink) -> None:
        self._completion_sink = sink

    async def dbos_prepare(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """DBOS prepare step: reconstruct Operation and run _prepare_task."""
        raw = payload.get("operation") if isinstance(payload.get("operation"), Mapping) else {}
        operation = Operation(
            request_id=str(raw.get("request_id") or ""),
            name=str(raw.get("name") or ""),
            args=dict(raw.get("args") or {}),
            origin=str(raw.get("origin") or "cli"),
            confirmed=bool(raw.get("confirmed")),
            delivery=dict(raw["delivery"]) if raw.get("delivery") else None,
        )
        prepared = await self._prepare_task(operation)
        if isinstance(prepared, OperationResult):
            return {"kind": "result", "result": prepared.as_dict()}
        return {"kind": "prepared", "payload": prepared.to_payload()}

    async def dbos_dispatch(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """DBOS dispatch step: reconstruct PreparedTask and dispatch it."""
        async def fail(code, message, *, state="failed", keep_lease=False):
            target = SessionRef(
                str(payload["target"]["host"]),
                str(payload["target"]["runtime"]),
                str(payload["target"]["session_id"]),
            )
            task_id = str(payload["task_id"])
            if not keep_lease:
                await asyncio.to_thread(self._arbiter.release, target, task_id)
            await self._transition(
                task_id, state,
                origin=str(payload["operation"].get("origin") or "cli"),
                request_id=str(payload["operation"].get("request_id") or ""),
                host=target.host, runtime=target.runtime, session_id=target.session_id,
                workspace=str(payload["workspace"]), error=f"{code}: {message}",
            )
            return OperationResult.failure(code, message, data={
                "task_id": task_id, "state": state, "host": target.host,
                "runtime": target.runtime, "session_id": target.session_id,
                "workspace": str(payload["workspace"]),
            })

        prepared = PreparedTask.from_payload(payload, fail)
        return (await self._dispatch_prepared(prepared)).as_dict()

    async def _transition(self, task_id: str, state: str, **fields: Any) -> dict[str, Any]:
        for _ in range(4):
            existing = await asyncio.to_thread(self._result_store.get, task_id) or {}
            current = str(existing.get("state") or "new")
            if current == state:
                return existing
            if not _can_transition(current, state):
                raise RuntimeError(f"invalid task state transition: {current} -> {state}")
            changed = await asyncio.to_thread(
                self._result_store.transition, task_id, current, state, **fields,
            )
            if changed:
                return await asyncio.to_thread(self._result_store.get, task_id) or {}
        raise RuntimeError(f"task state transition raced: {task_id} -> {state}")

    async def _prepare_task(self, operation: Operation) -> PreparedTask | OperationResult:
        try:
            await self.start()
        except Exception:
            LOGGER.exception("task runtime failed to start")
            return OperationResult.failure("runtime_unavailable", "task runtime failed to start")
        host = str(operation.args.get("host") or "")
        session = str(operation.args.get("session") or "")
        workspace = operation.args.get("workspace")
        text = operation.args.get("text")
        # Target existence is checked by the exact lookup below; the owner
        # start-turn is the authoritative injectability check. Do not let a
        # partial advisory probe reject a usable session.
        if not host or not session or not isinstance(text, str) or not text.strip():
            return OperationResult.failure("invalid_args", "task.prompt requires host, workspace, session, text")
        if workspace is None:
            return OperationResult.failure("invalid_args", "task.prompt requires workspace")
        workspace = str(workspace)
        row = await self._lookup(host, workspace, session)
        if row is None:
            return OperationResult.failure("target_not_found", "session was not found in the authorized catalog")
        cwd = PurePosixPath(str(row["cwd"]))
        if cwd != PurePosixPath(workspace):
            return OperationResult.failure("workspace_not_registered", "session workspace does not match")
        runtime = str(row["runtime"])
        task_id = self._task_id_for(operation)
        existing = await asyncio.to_thread(self._result_store.get, task_id)
        if existing is not None:
            replay = self._replay_result(
                existing, task_id, host, runtime, session, workspace, text,
            )
            if (
                str(existing.get("state") or "") == "submitted"
                and replay.code == "submission_uncertain"
            ):
                try:
                    await self._transition(
                        task_id, "uncertain", origin=operation.origin,
                        request_id=operation.request_id, host=host,
                        runtime=runtime, session_id=session, workspace=workspace,
                    )
                except Exception:
                    LOGGER.exception(
                        "Submitted task could not be marked uncertain on replay: %s",
                        task_id,
                    )
                else:
                    replay = OperationResult.failure(
                        "submission_uncertain",
                        "该请求已记录，但提交尚未确认；请先核对任务状态，避免重复执行。",
                        data={"task_id": task_id, "state": "uncertain"},
                    )
            return replay
        open_tasks = await asyncio.to_thread(
            self._result_store.open_tasks_for_target, host, runtime, session,
        )
        blocking = self._blocking_task(open_tasks)
        if blocking is not None:
            return OperationResult.failure(
                "session_busy",
                f"目标会话存在未终态任务 {blocking.get('task_id')}（{blocking.get('state')}）。"
                "请先核对任务状态；直接重发可能造成重复执行。",
                data={
                    "task_id": str(blocking.get("task_id") or ""),
                    "state": str(blocking.get("state") or ""),
                    "host": host, "runtime": runtime, "session_id": session,
                },
            )
        for legacy in open_tasks:
            LOGGER.warning(
                "Dispatching over a pre-runtime unfinished task %s (%s); verify the target session.",
                str(legacy.get("task_id") or "")[:8], legacy.get("state"),
            )
        target = SessionRef(host, runtime, session)
        try:
            await asyncio.to_thread(self._arbiter.acquire, target, task_id, "daemon")
        except SessionBusy as busy:
            return OperationResult.failure(
                "session_busy", f"session is executing task {busy.task_id}",
            )
        async def submission_failure(
            code: str, message: str, *, state: str = "failed", keep_lease: bool = False,
        ) -> OperationResult:
            if not keep_lease:
                await asyncio.to_thread(self._arbiter.release, target, task_id)
            await self._transition(
                task_id, state, origin=operation.origin, request_id=operation.request_id,
                host=target.host, runtime=target.runtime, session_id=target.session_id,
                workspace=workspace, error=f"{code}: {message}",
            )
            return OperationResult.failure(code, message, data={
                "task_id": task_id, "state": state, "host": target.host,
                "runtime": target.runtime, "session_id": target.session_id,
                "workspace": workspace,
            })

        fingerprint = hashlib.sha256(text.encode("utf-8")).hexdigest()
        claimed = await asyncio.to_thread(
            self._result_store.claim,
            task_id, origin=operation.origin, request_id=operation.request_id,
            host=host, runtime=str(row["runtime"]), session_id=session, workspace=workspace,
            recent_since=self._started_at, request_fingerprint=fingerprint,
        )
        if claimed is None:
            # This invocation acquired the lease before the atomic claim. It
            # must not leave that lease behind when another invocation won.
            await asyncio.to_thread(self._arbiter.release, target, task_id)
            existing = await asyncio.to_thread(self._result_store.get, task_id)
            if existing is not None:
                return self._replay_result(
                    existing, task_id, host, str(row["runtime"]), session, workspace, text,
                )
            current = await asyncio.to_thread(
                self._result_store.open_tasks_for_target,
                host, str(row["runtime"]), session,
            )
            blocking = self._blocking_task(current) or (
                current[0] if current else {"task_id": "", "state": "busy"}
            )
            return OperationResult.failure(
                "session_busy",
                f"目标会话存在未终态任务 {blocking.get('task_id')}（{blocking.get('state')}）。"
                "请先核对任务状态；直接重发可能造成重复执行。",
                data={
                    "task_id": str(blocking.get("task_id") or ""),
                    "state": str(blocking.get("state") or ""),
                    "host": host, "runtime": str(row["runtime"]), "session_id": session,
                },
            )
        if operation.delivery is not None:
            deliveries = await asyncio.to_thread(self._deliveries)
            await asyncio.to_thread(
                deliveries.record, task_id, operation.origin, operation.delivery,
            )
        if self._prompt_formatter is None:
            formatted = self._format_prompt(text)
        else:
            try:
                formatted = await self._prompt_formatter.format(
                    target, TaskDescriptor(text, text),
                )
            except PortError as error:
                return await submission_failure("prompt_format_failed", str(error))
            except Exception:
                LOGGER.warning("task prompt formatter failed")
                return await submission_failure("prompt_format_failed", "任务 Prompt 格式化失败，任务未派发。")
        try:
            validate_prompt_markdown(formatted)
        except ValueError as error:
            return await submission_failure("invalid_args", str(error))
        delivery = DeliveryContext("oc_cli", "cli", task_id)
        return PreparedTask(
            operation=operation, target=target, task_id=task_id, host=host,
            runtime=runtime, session=session, workspace=workspace, cwd=cwd,
            row=row, text=text, formatted=formatted, delivery=delivery,
            fail=submission_failure,
        )


    async def _dispatch_prepared(self, prepared: PreparedTask) -> OperationResult:
        operation = prepared.operation
        target = prepared.target
        task_id = prepared.task_id
        host = prepared.host
        runtime = prepared.runtime
        session = prepared.session
        workspace = prepared.workspace
        cwd = prepared.cwd
        row = prepared.row
        text = prepared.text
        formatted = prepared.formatted
        delivery = prepared.delivery
        submission_failure = prepared.fail
        if self._execution_port is None or self._journal is None:
            return await submission_failure("runtime_unavailable", "task runtime is not configured")
        if self._registry and PurePosixPath(str(cwd)) not in self._registry.get(host, ()):
            return await submission_failure("workspace_revoked", "目标工作区未授权或已移除。")

        journal = self._journal
        try:
            entry = await asyncio.to_thread(journal.claim, delivery.message_id)
            if entry is None:
                existing_entry = await asyncio.to_thread(
                    journal.get, delivery.message_id,
                )
                existing = await asyncio.to_thread(self._result_store.get, task_id)
                state = str((existing or {}).get("state") or "unknown")
                stage = str(getattr(existing_entry, "stage", "") or "")
                if stage in {"execution_started", "execution_queued"}:
                    try:
                        lease = await asyncio.to_thread(self._arbiter.lease, target)
                        owner = str((lease or {}).get("task_id") or "")
                        expires = float((lease or {}).get("expires_at") or 0)
                        if owner != task_id or expires <= time.time():
                            await asyncio.to_thread(
                                self._arbiter.acquire, target, task_id, "daemon",
                            )
                    except SessionBusy:
                        return OperationResult.failure(
                            "session_busy", "目标会话已由另一个任务持有，未重复投递。",
                        )
                    if state == "submitted":
                        await self._transition(
                            task_id, "running", origin=operation.origin,
                            request_id=operation.request_id, host=host,
                            runtime=runtime, session_id=session,
                            workspace=workspace,
                        )
                    self._start_heartbeat(target, task_id)
                    return OperationResult.success({
                        "task_id": task_id, "state": "running",
                        "request_id": operation.request_id,
                        "prompt_markdown": formatted,
                    }, f"task submitted: {task_id}")
                if stage == "uncertain" or state in {"submitted", "uncertain"}:
                    if state == "submitted":
                        try:
                            await self._transition(
                                task_id, "uncertain", origin=operation.origin,
                                request_id=operation.request_id, host=host,
                                runtime=runtime, session_id=session,
                                workspace=workspace,
                            )
                        except Exception:
                            LOGGER.exception(
                                "Replayed dispatch could not mark task uncertain: %s",
                                task_id,
                            )
                    return OperationResult.failure(
                        "submission_uncertain",
                        "该请求已记录，但提交尚未确认；请先核对任务状态，避免重复执行。",
                        data={"task_id": task_id, "state": "uncertain"},
                    )
                return OperationResult.success({
                    "task_id": task_id, "state": state,
                    "request_id": operation.request_id,
                    "prompt_markdown": formatted,
                }, f"task already recorded: {task_id}")
            await asyncio.to_thread(
                journal.record_delivery_chat, delivery.message_id, delivery.chat_id,
            )
            await asyncio.to_thread(journal.set_prompt, delivery.message_id, formatted)
            digest = hashlib.sha256(formatted.encode("utf-8")).hexdigest()
            await asyncio.to_thread(
                journal.advance, delivery.message_id, "preview_ready",
                preview_sha256=digest, host=host, thread_id=session,
                project_root=str(cwd), runtime=str(row["runtime"]),
            )
            await asyncio.to_thread(
                journal.advance, delivery.message_id, "preview_delivered",
            )
            entry = await asyncio.to_thread(
                journal.advance, delivery.message_id, "execution_submitting",
            )
        except (JournalConflict, ValueError, OSError):
            LOGGER.warning("task journal preparation failed")
            return await submission_failure("upstream_error", "任务执行记录失败，任务未提交。")

        try:
            receipt = await self._execution_port.submit(
                target, str(cwd), formatted, entry.execution_request_id,
            )
        except Exception:
            LOGGER.warning("task dispatch failed")
            return await submission_failure("upstream_error", "任务投递失败，请检查目标会话的连接和状态。")

        if receipt.state == "uncertain":
            try:
                await asyncio.to_thread(journal.advance, delivery.message_id, "uncertain")
            except (JournalConflict, ValueError):
                LOGGER.warning("task journal uncertain transition failed")
            return await submission_failure(
                "submission_uncertain",
                "提交尚未确认，目标会话可能已收到任务。请先核对任务状态，避免重复提交。",
                state="uncertain", keep_lease=True,
            )
        if receipt.state == "rejected":
            try:
                await asyncio.to_thread(journal.advance, delivery.message_id, "failed")
            except (JournalConflict, ValueError):
                LOGGER.warning("task journal rejection transition failed")
            return await submission_failure(
                receipt.error_code or "upstream_error",
                receipt.error_message or "目标会话拒绝了任务，请检查授权和会话状态。",
                state="rejected",
            )

        association_error: str | None = None
        try:
            if str(row["runtime"]) == "dsh":
                await asyncio.to_thread(journal.advance, delivery.message_id, "execution_queued")
            else:
                await asyncio.to_thread(
                    journal.advance, delivery.message_id, "execution_started",
                    execution_turn_id=str(receipt.execution_ref or ""),
                )
        except (JournalConflict, ValueError) as error:
            association_error = str(error)
            LOGGER.warning(
                "task %s accepted but journal association failed: %s", task_id, error,
            )

        await self._transition(
            task_id, "running", origin=operation.origin, request_id=operation.request_id,
            host=host, runtime=str(row["runtime"]), session_id=session, workspace=workspace,
        )
        self._start_heartbeat(target, task_id)
        data = {
            "task_id": task_id, "state": "running",
            "request_id": operation.request_id, "prompt_markdown": formatted,
        }
        if association_error is not None:
            data["association_error"] = association_error
        return OperationResult.success(data, f"task submitted: {task_id}")

    async def status(self, operation: Operation) -> OperationResult:
        task_id = str(operation.args.get("task_id") or "")
        result = await asyncio.to_thread(self._result_store.get, task_id)
        if result is None:
            return OperationResult.failure("task_not_found", "task was not found")
        data = dict(result)
        data.update(await asyncio.to_thread(self._observe, task_id, result))
        try:
            outbox = await asyncio.to_thread(
                lambda: self._deliveries().outbox(task_id),
            )
        except Exception:
            outbox = None
        if outbox is not None:
            data["egress"] = {
                "state": str(outbox.get("state") or ""),
                "attempts": int(outbox.get("attempts") or 0),
                "last_error": outbox.get("last_error"),
            }
        return OperationResult.success(data)

    @staticmethod
    def _task_id_for(operation: Operation) -> str:
        """Use the caller's stable request id when safe, else a random id."""
        request_id = operation.request_id
        if (
            isinstance(request_id, str) and request_id
            and request_id.isascii() and request_id.isprintable()
            and " " not in request_id
            and len(request_id.encode("utf-8")) <= 256
        ):
            return request_id
        return uuid4().hex

    def _replay_result(
        self, existing: Mapping[str, Any], task_id: str,
        host: str, runtime: str, session: str, workspace: str, text: str | None = None,
    ) -> OperationResult:
        stored = (
            existing.get("host"), existing.get("runtime"),
            existing.get("session_id"), existing.get("workspace"),
        )
        supplied = (host, runtime, session, workspace)
        if any(isinstance(old, str) and old and old != new for old, new in zip(stored, supplied)):
            return OperationResult.failure(
                "invalid_args",
                "该请求标识已用于不同的目标或工作区，未派发。",
                data={"task_id": task_id, "state": str(existing.get("state") or "")},
            )
        if text is not None:
            stored_fingerprint = self._result_store.request_fingerprint(task_id)
            if (
                stored_fingerprint is not None
                and stored_fingerprint != hashlib.sha256(text.encode("utf-8")).hexdigest()
            ):
                return OperationResult.failure(
                    "invalid_args",
                    "该请求标识已用于不同的任务正文，未派发。",
                    data={"task_id": task_id, "state": str(existing.get("state") or "")},
                )
        state = str(existing.get("state") or "")
        if state in {"submitted", "uncertain"}:
            # The first invocation claimed the task but did not prove a
            # dispatch. A replay must not claim success (N-22/N-20).
            return OperationResult.failure(
                "submission_uncertain",
                "该请求已记录，但提交尚未确认；请先核对任务状态，避免重复执行。",
                data={"task_id": task_id, "state": state},
            )
        return OperationResult.success(
            {"task_id": task_id, "state": state, "duplicate": True},
            f"task already recorded: {task_id}",
        )

    def _observe(self, task_id: str, row: Mapping[str, Any]) -> dict[str, object]:
        """Derive observation facts from durable state; never write the task."""
        stage: str | None = None
        if self._journal is not None:
            entry = self._journal.get(task_id)
            if entry is not None:
                stage = entry.stage
        host = str(row.get("host") or "")
        runtime = str(row.get("runtime") or "")
        session = str(row.get("session_id") or "")
        lease_task_id: str | None = None
        lease_expires_at: float | None = None
        heartbeat_at: float | None = None
        if host and runtime and session:
            lease = self._arbiter.lease(SessionRef(host, runtime, session))
            if lease is not None:
                lease_task_id = str(lease.get("task_id") or "")
                expires = lease.get("expires_at")
                if isinstance(expires, (int, float)) and not isinstance(expires, bool):
                    lease_expires_at = float(expires)
                heartbeat = lease.get("heartbeat_at")
                if isinstance(heartbeat, (int, float)) and not isinstance(heartbeat, bool):
                    heartbeat_at = float(heartbeat)
            if lease_expires_at is not None and lease_expires_at <= time.time():
                lease_task_id, lease_expires_at, heartbeat_at = None, None, None
        return describe_observation(
            row, journal_stage=stage, lease_task_id=lease_task_id,
            lease_expires_at=lease_expires_at, runtime_started_at=self._started_at,
            now=time.time(), heartbeat_at=heartbeat_at,
            heartbeat_stale_after=self._heartbeat_stale_after,
            observation_window_seconds=self._observation_window_seconds,
        ).as_fields()

    def _blocking_task(
        self, tasks: list[Mapping[str, Any]],
    ) -> Mapping[str, Any] | None:
        """Return the record that must block a new dispatch, if any.

        Any uncertain record wins over runtime-owned running records, and a
        newer legacy running record may not hide an older uncertain one.
        """
        for task in tasks:
            if str(task.get("state") or "") == "uncertain":
                return task
        for task in tasks:
            if self._should_guard_target(task):
                return task
        return None

    def _should_guard_target(self, open_task: Mapping[str, Any]) -> bool:
        """Decide whether an unfinished task must block a new dispatch.

        Tasks created before this runtime started belong to the user's
        display-only compatibility decision: they stay visible as interrupted
        but must not freeze their targets forever. Uncertain tasks always
        block because the target may already have executed them.
        """
        if str(open_task.get("state") or "") == "uncertain":
            return True
        if str(open_task.get("task_id") or "") in self._recovered:
            return True
        created = open_task.get("created_at")
        if self._started_at is None or isinstance(created, bool) or not isinstance(created, (int, float)):
            return False
        return float(created) >= self._started_at

    def _start_heartbeat(self, target: SessionRef, task_id: str) -> None:
        interval = self._heartbeat_interval
        if interval <= 0 or task_id in self._heartbeats:
            return
        self._heartbeats[task_id] = asyncio.create_task(
            self._heartbeat_loop(target, task_id, interval),
        )

    async def _heartbeat_loop(self, target: SessionRef, task_id: str, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                renewed = await asyncio.to_thread(self._arbiter.heartbeat, target, task_id)
                if not renewed:
                    LOGGER.warning("Lease heartbeat stopped; the lease was lost for %s.", task_id[:8])
                    return
        except asyncio.CancelledError:
            raise
        finally:
            self._heartbeats.pop(task_id, None)

    def _stop_heartbeat(self, task_id: str) -> None:
        task = self._heartbeats.pop(task_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def result(self, operation: Operation) -> OperationResult:
        task_id = str(operation.args.get("task_id") or "")
        row = await asyncio.to_thread(self._result_store.get, task_id)
        if row is None:
            return OperationResult.failure("task_not_found", "task was not found")
        state = str(row.get("state") or "unknown")
        output = row.get("output") if isinstance(row.get("output"), str) else None
        error = row.get("error") if isinstance(row.get("error"), str) else None
        target = SessionRef(
            str(row.get("host") or "local"), str(row.get("runtime") or "codex"),
            str(row.get("session_id") or ""),
        )
        outcome = output.strip().splitlines()[0][:200] if output and output.strip() else state
        brief = ExecutionBrief(
            task_id=task_id, target=target, state=state, outcome=outcome,
            blockers=(error,) if error else (),
        )
        source = "deterministic"
        if self._summarize is not None and state in {"completed", "failed"}:
            prompt = ""
            if self._journal is not None:
                entry = await asyncio.to_thread(self._journal.get, task_id)
                prompt = entry.prompt or "" if entry is not None else ""
            evidence = ExecutionEvidence(
                task_id, target, state, "task result", output=output, error=error,
            )
            try:
                brief = await self._summarize(evidence, prompt)
                source = "llm"
            except Exception:
                LOGGER.warning("task brief LLM summarization failed")
                source = "fallback"
        data = asdict(brief)
        data["brief_source"] = source
        data["output"] = output
        data["error"] = error
        data.update(await asyncio.to_thread(self._observe, task_id, row))
        return OperationResult.success(data)

    async def resolve(self, operation: Operation) -> OperationResult:
        """Explicit user disposition for an unverifiable task; no remote kill."""
        task_id = str(operation.args.get("task_id") or "")
        if not task_id:
            return OperationResult.failure("invalid_args", "task.resolve requires task_id")
        row = await asyncio.to_thread(self._result_store.get, task_id)
        if row is None:
            return OperationResult.failure("task_not_found", "task was not found")
        state = str(row.get("state") or "")
        if state in {"completed", "failed", "rejected", "resolved"}:
            return OperationResult.failure(
                "invalid_args", f"task is already terminal: {state}",
            )
        if state != "uncertain":
            return OperationResult.failure(
                "invalid_args",
                f"task is not uncertain: {state}; task.resolve only accepts uncertain tasks",
            )
        reason = operation.args.get("reason")
        reason = reason.strip()[:200] if isinstance(reason, str) and reason.strip() else None
        resolved = await asyncio.to_thread(
            self._result_store.resolve, task_id, reason=reason,
        )
        if resolved is None:
            return OperationResult.failure(
                "invalid_args", "task is no longer resolvable",
            )
        target = SessionRef(
            str(row.get("host") or "local"), str(row.get("runtime") or "codex"),
            str(row.get("session_id") or ""),
        )
        try:
            await asyncio.to_thread(self._arbiter.release, target, task_id)
        except Exception:
            LOGGER.warning("Resolved task lease could not be released")
        self._recovered.discard(task_id)
        LOGGER.info("Task %s resolved (was %s).", task_id[:8], state)
        return OperationResult.success(
            {
                "task_id": task_id, "state": "resolved",
                "previous_state": state, "reason": reason,
            },
            "任务已在网关侧解除，未终止远端执行；请自行核对目标会话。",
        )

    def handlers(self) -> dict[str, Callable[[Operation], Awaitable[OperationResult]]]:
        return {
            "task.status": self.status,
            "task.result": self.result,
            "task.resolve": self.resolve,
        }
