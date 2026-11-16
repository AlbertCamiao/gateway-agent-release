"""Read-only daemon health for diagnostics."""

from __future__ import annotations

import asyncio


async def service_status(program=None) -> dict[str, str]:
    try:
        from feishu_gateway.daemon import DaemonClient, DaemonPaths

        health = await asyncio.to_thread(
            DaemonClient(DaemonPaths.from_home()).health,
        )
    except Exception:
        return {"process": "unknown", "feishu": "unknown"}
    running = bool(health.get("ok") or health.get("rpc"))
    return {
        "process": "running" if running else "stopped",
        "feishu": "unknown",
    }
