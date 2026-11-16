"""Routing data types shared by the gateway modules."""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

from feishu_gateway.contracts import SessionRef


RouteIntent = Literal["query", "task"]
SessionRuntime = Literal["codex", "dsh"]


INTERACTIVE_SOURCES = ("appServer", "cli", "vscode")


@dataclass(frozen=True, slots=True)
class SessionCandidate:
    target: SessionRef
    title: str | None
    cwd: Path


@dataclass(frozen=True, slots=True)
class RoutedInstruction:
    target: SessionRef
    instruction: str
    target_phrase: str | None = None
    intent: RouteIntent = "task"
    cwd: Path | PurePosixPath | None = None
    prompt_markdown: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    candidates: tuple["SessionCandidate", ...]
    unavailable_hosts: frozenset[str] = frozenset()
    unavailable_backends: frozenset[tuple[SessionRuntime, str]] = frozenset()
