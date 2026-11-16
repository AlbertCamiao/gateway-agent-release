"""Keep reverse completion tunnels alive for configured SSH hosts."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
from pathlib import Path

from feishu_gateway import config
from feishu_gateway.home import GatewayHome


LOGGER = logging.getLogger("feishu-gateway")


class CompletionTunnelManager:
    def __init__(
        self, home: GatewayHome, port: int, *, interval: float = 10.0,
        scopes_provider=None, runner=None,
    ) -> None:
        self.home = home
        self.port = port
        self.interval = interval
        self._scopes_provider = scopes_provider or (
            lambda: config.read_ssh_scopes(home.ssh_config_path)
        )
        self._runner = runner or subprocess.run
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            await self.ensure_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                break
            except TimeoutError:
                pass

    async def ensure_once(self) -> None:
        try:
            scopes = await asyncio.to_thread(self._scopes_provider)
        except Exception:
            LOGGER.warning("completion tunnel scopes unavailable")
            return
        for scope in scopes:
            transport = scope.transport
            control = transport.control_path
            if control is None:
                continue
            try:
                alive = await asyncio.to_thread(transport.ensure_control_master)
            except Exception:
                alive = False
            if not alive:
                continue
            command = self._forward_command(control, transport.ssh_alias or transport.host)
            environment = {
                name: os.environ[name]
                for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
                if name in os.environ
            }
            try:
                result = await asyncio.to_thread(
                    self._runner, command, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, timeout=10, check=False, env=environment,
                )
            except Exception:
                LOGGER.warning("completion tunnel refresh failed")
                continue
            if getattr(result, "returncode", 1) != 0:
                # An existing forward returns a non-zero status on some OpenSSH
                # versions; the synthetic probe is the source of truth.
                LOGGER.debug("completion tunnel forward was not refreshed")

    def _forward_command(self, control: Path, alias: str) -> list[str]:
        return [
            "/usr/bin/ssh", "-S", str(control), "-O", "forward",
            "-R", f"127.0.0.1:{self.port}:127.0.0.1:{self.port}",
            "--", alias,
        ]
