"""Filesystem layout for source checkouts and installed distributions."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path


_SOURCE_ROOT = Path(__file__).resolve().parents[2]

def _default_installed_root() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "gateway-agent"
    base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / "gateway-agent"


def _is_source_checkout() -> bool:
    return (_SOURCE_ROOT / "pyproject.toml").is_file() and (
        _SOURCE_ROOT / "src" / "feishu_gateway"
    ).is_dir()


def _write_private(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class GatewayHome:
    root: Path
    config_dir: Path
    data_dir: Path
    log_dir: Path
    source: bool

    @classmethod
    def discover(cls) -> "GatewayHome":
        override = os.environ.get("FEISHU_GATEWAY_HOME")
        if override:
            root = Path(override).expanduser().resolve()
            return cls(root, root / "config", root / "data", root / "logs", False)
        if _is_source_checkout():
            # Source checkouts keep configuration next to the checkout, but
            # runtime databases/sockets live under data/ so they do not litter
            # the repository root.
            return cls(
                _SOURCE_ROOT, _SOURCE_ROOT / "config", _SOURCE_ROOT / "data",
                _SOURCE_ROOT / "logs", True,
            )
        root = _default_installed_root().expanduser().resolve()
        return cls(root, root / "config", root / "data", root / "logs", False)

    @property
    def env_path(self) -> Path:
        return self.config_dir / ".env"

    @property
    def config_path(self) -> Path:
        return self.config_dir / ".gateway-preview.json"

    @property
    def daemon_data_dir(self) -> Path:
        return self.data_dir / "daemon"

    @property
    def ingress_data_dir(self) -> Path:
        return self.data_dir / "ingress"

    @property
    def daemon_state_path(self) -> Path:
        return self.daemon_data_dir / "gateway.sqlite3"

    @property
    def ssh_config_path(self) -> Path:
        return self.config_dir / ".gateway-ssh.json"

    @property
    def dsh_config_path(self) -> Path:
        return self.config_dir / ".gateway-dsh.json"

    @property
    def listener_lock_path(self) -> Path:
        return self.daemon_data_dir / ".gateway-listener.lock"

    @property
    def registry_path(self) -> Path:
        return self.config_dir / ".gateway-workspaces.json"

    @property
    def daemon_config_path(self) -> Path:
        return self.config_dir / "daemon.json"

    @property
    def ingress_config_path(self) -> Path:
        return self.config_dir / "ingress.json"

    @property
    def ingress_binding_path(self) -> Path:
        return self.ingress_data_dir / "ingress-binding.json"

    @property
    def ingress_challenge_path(self) -> Path:
        return self.ingress_data_dir / "ingress-challenge.json"

    @property
    def completion_state_path(self) -> Path:
        return self.config_dir / ".gateway-completion.json"

    @property
    def daemon_socket_path(self) -> Path:
        return self.daemon_data_dir / ".gateway-daemon.sock"

    @property
    def daemon_lock_path(self) -> Path:
        return self.daemon_data_dir / ".gateway-daemon.lock"

    @property
    def daemon_pid_path(self) -> Path:
        return self.daemon_data_dir / ".gateway-daemon.pid"

    @property
    def daemon_log_path(self) -> Path:
        return self.log_dir / ".gateway-daemon.log"

    def ensure_dirs(self) -> None:
        for directory in (
            self.root, self.config_dir, self.data_dir, self.log_dir,
            self.daemon_data_dir, self.ingress_data_dir,
        ):
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chmod(directory, 0o700)
