"""Adapters that implement connect_ports using existing I/O modules."""

from __future__ import annotations

import asyncio
from pathlib import Path

from feishu_gateway import config
from feishu_gateway.bridge.apply import BridgeApplyError, apply_bridge_only
from feishu_gateway.bridge.status import BridgeStatusError
from feishu_gateway.completion.sync import sync_endpoint
from feishu_gateway.execution.connect_ports import (
    BridgeResult,
    CompletionResult,
    ConnectError,
)
from feishu_gateway.workspaces.registry import WorkspaceRegistryError
from feishu_gateway.workspaces.target import host_for


class HostRegistryAdapter:
    def __init__(self, config_path: Path = config.SSH_CONFIG_PATH) -> None:
        self._config_path = config_path

    async def resolve(self, host: str) -> str:
        try:
            scopes = await asyncio.to_thread(config.read_ssh_scopes, self._config_path)
            resolved, transport = await asyncio.to_thread(host_for, host, scopes)
        except (WorkspaceRegistryError, config.GatewayConfigError, ValueError, OSError):
            raise ConnectError("host_not_configured", "主机未配置或不可唯一解析。") from None
        if resolved == "local" or transport is None:
            raise ConnectError("invalid_args", "远端连接不能使用 local。")
        return resolved


class BridgeAdapter:
    async def ensure_bridge(self, host: str, *, profile: str | None = None) -> BridgeResult:
        try:
            result = await asyncio.to_thread(apply_bridge_only, host, profile)
        except (BridgeApplyError, BridgeStatusError, OSError):
            raise ConnectError("upstream_error", "DSH bridge 配置失败。") from None
        after = result.get("after") or {}
        return BridgeResult(
            str(after.get("host") or host),
            bool(result.get("changed")),
            profile,
            ("DSH: reload the panel to load the new bridge.",) if result.get("changed") else (),
        )


class CompletionAdapter:
    async def sync(self, host: str, *, include_dsh: bool = True) -> CompletionResult:
        try:
            result = await asyncio.to_thread(sync_endpoint, host, dsh=include_dsh)
        except (OSError, ValueError):
            raise ConnectError("upstream_error", "completion endpoint 同步失败。") from None
        return CompletionResult(
            str(result["host"]), int(result["port"]),
            bool(result.get("dsh")), str(result.get("tunnel") or "unknown"),
        )
