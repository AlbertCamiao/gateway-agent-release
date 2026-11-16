"""Host connection orchestration; depends only on connect_ports."""

from __future__ import annotations

from feishu_gateway.execution.connect_ports import (
    BridgeConnector,
    CodexHookInstaller,
    CompletionEndpoint,
    ConnectError,
    ConnectResult,
    HostResolver,
)


class HostConnectService:
    def __init__(
        self, *,
        hosts: HostResolver,
        bridge: BridgeConnector,
        completion: CompletionEndpoint,
        hooks: CodexHookInstaller | None = None,
    ) -> None:
        self._hosts = hosts
        self._bridge = bridge
        self._completion = completion
        self._hooks = hooks

    async def connect(
        self, host: str, *, profile: str | None = None,
        include_dsh: bool = True, include_codex: bool = True,
    ) -> ConnectResult:
        if not isinstance(host, str) or not host.strip() or len(host) > 253:
            raise ConnectError("invalid_args", "host is invalid")
        resolved = await self._hosts.resolve(host.strip())
        bridge = None
        if include_dsh:
            bridge = await self._bridge.ensure_bridge(resolved, profile=profile)
        completion = await self._completion.sync(resolved, include_dsh=include_dsh)
        hook = None
        if include_codex:
            if self._hooks is None:
                raise ConnectError("upstream_error", "Codex Hook installer is unavailable")
            hook = await self._hooks.install(resolved)
        steps = []
        for result in (bridge, completion, hook):
            if result is not None:
                steps.extend(getattr(result, "manual_steps", ()))
        if hook is not None and hook.trust_pending:
            steps.append("Codex: review and trust the Stop hook once.")
        return ConnectResult(
            resolved, bridge=bridge, completion=completion, hook=hook,
            manual_steps=tuple(dict.fromkeys(steps)),
        )
