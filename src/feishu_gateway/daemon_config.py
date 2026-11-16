"""Validated static daemon config and a failure-preserving file watcher."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


DEFAULTS: dict[str, Any] = {
    "log_level": "INFO",
    "completion_port": 0,
    "reload_interval": 1.0,
}


class DaemonConfigError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DaemonConfig:
    log_level: str = "INFO"
    completion_port: int = 0
    reload_interval: float = 1.0


def read_daemon_config(path: Path) -> DaemonConfig:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return DaemonConfig()
    except (OSError, UnicodeError, ValueError) as error:
        raise DaemonConfigError("daemon.json is invalid") from error
    if not isinstance(raw, dict):
        raise DaemonConfigError("daemon.json must be an object")
    unknown = set(raw) - set(DEFAULTS)
    if unknown:
        raise DaemonConfigError("daemon.json has unknown keys")
    log_level = raw.get("log_level", DEFAULTS["log_level"])
    port = raw.get("completion_port", DEFAULTS["completion_port"])
    interval = raw.get("reload_interval", DEFAULTS["reload_interval"])
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise DaemonConfigError("invalid log_level")
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
        raise DaemonConfigError("invalid completion_port")
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) or not 0.2 <= interval <= 60:
        raise DaemonConfigError("invalid reload_interval")
    return DaemonConfig(str(log_level), int(port), float(interval))


class ConfigWatcher:
    def __init__(
        self, path: Path, loader: Callable[[Path], Any] = read_daemon_config,
        *, interval: float = 1.0, on_change: Callable[[Any], None] | None = None,
    ) -> None:
        self.path = path
        self.loader = loader
        self.interval = interval
        self.value: Any = None
        self.error: str | None = None
        self._digest: int | None = None
        self.on_change = on_change

    def _read(self) -> tuple[int | None, Any]:
        try:
            data = self.path.read_bytes()
            digest = hash(data)
        except FileNotFoundError:
            digest = None
            data = b""
        value = self.loader(self.path)
        return digest, value

    def load_once(self) -> None:
        try:
            digest, value = self._read()
        except Exception as error:
            self.error = type(error).__name__
            return
        if digest == self._digest:
            return
        self.value = value
        self.error = None
        self._digest = digest
        self.interval = float(getattr(value, "reload_interval", self.interval))
        if self.on_change is not None:
            try:
                self.on_change(value)
            except Exception as error:
                self.error = type(error).__name__

    async def run(self, stop: asyncio.Event | None = None) -> None:
        self.load_once()
        while stop is None or not stop.is_set():
            try:
                if stop is None:
                    await asyncio.sleep(self.interval)
                else:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval)
                    break
            except TimeoutError:
                pass
            try:
                digest, value = self._read()
            except Exception as error:
                self.error = type(error).__name__
                continue
            if digest == self._digest:
                continue
            try:
                self.value = value
                self.error = None
                self._digest = digest
            except Exception as error:
                self.error = type(error).__name__
                # Keep the previous valid value and digest.
