"""Ports for the host-connect service.

The service layer must not import bridge, SSH, completion-sync, or host_admin
implementations. Composition roots inject these adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True, slots=True)
class BridgeResult:
    host: str
    changed: bool
    profile: str | None = None
    manual_steps: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompletionResult:
    host: str
    port: int
    dsh_configured: bool
    tunnel: str


@dataclass(frozen=True, slots=True)
class HookResult:
    host: str
    installed: bool
    trust_pending: bool = True
    manual_steps: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConnectResult:
    host: str
    bridge: BridgeResult | None = None
    completion: CompletionResult | None = None
    hook: HookResult | None = None
    manual_steps: tuple[str, ...] = field(default_factory=tuple)


class ConnectError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class HostResolver(Protocol):
    async def resolve(self, host: str) -> str: ...


class BridgeConnector(Protocol):
    async def ensure_bridge(self, host: str, *, profile: str | None = None) -> BridgeResult: ...


class CompletionEndpoint(Protocol):
    async def sync(self, host: str, *, include_dsh: bool = True) -> CompletionResult: ...


class CodexHookInstaller(Protocol):
    async def install(self, host: str) -> HookResult: ...
