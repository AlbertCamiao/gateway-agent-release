"""Typed Operation envelope for the daemon/CLI boundary (R1.0 contract).

This module is intentionally runtime-free: it does not import the daemon,
CLI, adapters, or Feishu. It freezes the Operation surface that later gates
will wire into the daemon.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Mapping


Origin = Literal["cli", "console", "feishu"]
OperationKind = Literal["read", "write", "task"]

READ_ONLY_OPERATIONS = frozenset({
    "session.list",
    "session.refresh",
    "session.status",
    "workspace.list",
    "task.status",
    "task.result",
    "host.list",
    "host.candidates",
    "dsh.status",
    "dsh.plan",
    "doctor",
})

WRITE_OPERATIONS = frozenset({
    "workspace.register",
    "workspace.remove",
    "host.add",
    "host.remove",
    "dsh.connect",
    "connect",
    "completion.sync",
    "task.resolve",
})

TASK_OPERATIONS = frozenset({
    "task.prompt",
})

OPERATION_NAMES = READ_ONLY_OPERATIONS | WRITE_OPERATIONS | TASK_OPERATIONS
ORIGINS = frozenset({"cli", "console", "feishu"})

_FORBIDDEN_ARG_KEYS = frozenset({"shell", "argv", "command_line"})


@dataclass(frozen=True, slots=True)
class Operation:
    request_id: str
    name: str
    args: Mapping[str, object] = field(default_factory=dict)
    origin: Origin = "cli"
    confirmed: bool = False
    delivery: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValueError("request_id is required")
        if len(self.request_id.encode("utf-8")) > 256:
            raise ValueError("request_id is too long")
        if self.name not in OPERATION_NAMES:
            raise ValueError(f"unknown operation: {self.name}")
        if not isinstance(self.args, Mapping):
            raise ValueError("operation args must be a mapping")
        if any(not isinstance(key, str) for key in self.args):
            raise ValueError("operation arg keys must be strings")
        if _FORBIDDEN_ARG_KEYS.intersection(self.args):
            raise ValueError("raw shell payloads are not allowed")
        if self.origin not in ORIGINS:
            raise ValueError("unsupported operation origin")
        if not isinstance(self.confirmed, bool):
            raise ValueError("confirmed must be a boolean")
        if self.delivery is not None:
            if not isinstance(self.delivery, Mapping):
                raise ValueError("operation delivery must be a mapping")
            if not set(self.delivery) <= {"chat_id", "sender_id", "message_id"}:
                raise ValueError("unsupported operation delivery field")
            if any(
                not isinstance(value, str) or not value
                for value in self.delivery.values()
            ):
                raise ValueError("operation delivery values must be non-empty strings")
            object.__setattr__(
                self, "delivery", MappingProxyType(dict(self.delivery)),
            )
        object.__setattr__(self, "args", MappingProxyType(dict(self.args)))

    @property
    def kind(self) -> OperationKind:
        if self.name in READ_ONLY_OPERATIONS:
            return "read"
        if self.name in WRITE_OPERATIONS:
            return "write"
        return "task"

    @property
    def mutating(self) -> bool:
        return self.kind == "write"
