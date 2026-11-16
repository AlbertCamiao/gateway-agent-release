"""Shared session, task, and execution result contracts for the live gateway path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Literal, Mapping, Protocol, Sequence, runtime_checkable


PORT_ERROR_CODES = frozenset({
    "host_missing", "host_ambiguous", "host_not_configured",
    "workspace_missing", "workspace_ambiguous", "workspace_not_registered",
    "confirmation_required", "not_bot_allowed", "invalid_args", "scope_denied",
    "target_not_found", "target_ambiguous", "model_unavailable", "upstream_error",
    "internal_error", "invalid_protocol", "version_unsupported", "session_unavailable",
    "duplicate_event",
})


def _identifier(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    if not value.isprintable() or any(ch.isspace() for ch in value):
        raise ValueError(f"{name} must be a non-empty printable token")
    return value


@dataclass(frozen=True, slots=True)
class SessionRef:
    host: str
    runtime: str
    session_id: str

    def __post_init__(self) -> None:
        _identifier(self.host, "host")
        _identifier(self.runtime, "runtime")
        _identifier(self.session_id, "session_id")


@dataclass(frozen=True, slots=True)
class TaskDescriptor:
    summary: str
    prompt: str
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.summary, str) or not self.summary.strip():
            raise ValueError("task summary is required")
        if not isinstance(self.prompt, str) or not self.prompt.strip():
            raise ValueError("task prompt is required")
        if not all(isinstance(tag, str) and tag.strip() for tag in self.tags):
            raise ValueError("task tags must be non-empty strings")


class PortError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, retryable: bool = False,
        candidates: Sequence[str] = (), details: Mapping[str, object] | None = None,
    ) -> None:
        if code not in PORT_ERROR_CODES:
            raise ValueError(f"unknown port error code: {code}")
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.candidates = tuple(candidates)
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    chat_id: str
    sender_id: str
    message_id: str

    def __post_init__(self) -> None:
        for value, name in ((self.chat_id, "chat_id"), (self.sender_id, "sender_id"),
                            (self.message_id, "message_id")):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")


ExecutionStatus = Literal["submitted", "approval_pending", "completed", "failed", "unknown"]


@dataclass(frozen=True, slots=True)
class ExecutionObservation:
    turn_id: str | None
    status: ExecutionStatus
    text: str | None = None


@dataclass(frozen=True, slots=True)
class ExecutionEvidence:
    task_id: str
    target: SessionRef
    state: str
    task_summary: str
    output: str | None = None
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None


@dataclass(frozen=True, slots=True)
class ExecutionBrief:
    task_id: str
    target: SessionRef
    state: str
    outcome: str
    key_actions: tuple[str, ...] = ()
    artifacts: tuple[str, ...] = ()
    verification: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    next_steps: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Reply:
    text: str
    kind: str = "answer"
    message_id: str | None = None


SessionState = Literal["idle", "busy", "unknown", "offline"]
SubmissionState = Literal["accepted", "rejected", "uncertain"]
CompletionState = Literal["completed", "failed"]


@dataclass(frozen=True, slots=True)
class SessionSnapshot:
    target: SessionRef
    workspace: str
    reachable: bool
    state: SessionState
    observed_at: float
    title: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.target, SessionRef):
            raise ValueError("snapshot target must be a SessionRef")
        if not isinstance(self.workspace, str) or not self.workspace:
            raise ValueError("snapshot workspace is required")
        if not isinstance(self.reachable, bool):
            raise ValueError("snapshot reachable must be a boolean")
        if self.state not in {"idle", "busy", "unknown", "offline"}:
            raise ValueError("invalid session state")
        if isinstance(self.observed_at, bool) or not isinstance(self.observed_at, (int, float)):
            raise ValueError("snapshot observed_at must be a timestamp")
        if self.title is not None and (not isinstance(self.title, str) or not self.title.strip()):
            raise ValueError("snapshot title must be non-empty text")


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    state: SubmissionState
    execution_ref: str | None = None
    error_code: str | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.state not in {"accepted", "rejected", "uncertain"}:
            raise ValueError("invalid submission state")
        if self.execution_ref is not None and (
            not isinstance(self.execution_ref, str) or not self.execution_ref
        ):
            raise ValueError("execution_ref must be non-empty when present")
        if self.state == "accepted" and not self.execution_ref:
            raise ValueError("accepted submission requires execution_ref")


@dataclass(frozen=True, slots=True)
class CompletionEvent:
    event_id: str
    target: SessionRef
    execution_ref: str
    state: CompletionState
    occurred_at: float
    output: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("completion event_id is required")
        if not isinstance(self.target, SessionRef):
            raise ValueError("completion target must be a SessionRef")
        if not isinstance(self.execution_ref, str) or not self.execution_ref:
            raise ValueError("completion execution_ref is required")
        if self.state not in {"completed", "failed"}:
            raise ValueError("completion state must be completed or failed")
        if isinstance(self.occurred_at, bool) or not isinstance(self.occurred_at, (int, float)):
            raise ValueError("completion occurred_at must be a timestamp")
        if self.output is not None and not isinstance(self.output, str):
            raise ValueError("completion output must be text")
        if self.error is not None and not isinstance(self.error, str):
            raise ValueError("completion error must be text")


@runtime_checkable
class CompletionSource(Protocol):
    def events(self) -> AsyncIterator[CompletionEvent]: ...
    async def close(self) -> None: ...


@runtime_checkable
class ExecutionPort(Protocol):
    async def submit(
        self, target: SessionRef, workspace: str, prompt: str, request_id: str,
    ) -> SubmissionReceipt: ...

    async def list(
        self, *, hosts: frozenset[str], runtimes: frozenset[str],
        workspace: str | None = None,
    ) -> tuple[tuple[SessionSnapshot, ...], frozenset[str], frozenset[tuple[str, str]]]: ...

    async def status(self, target: SessionRef) -> SessionSnapshot: ...

    def completion_source(self) -> CompletionSource: ...
