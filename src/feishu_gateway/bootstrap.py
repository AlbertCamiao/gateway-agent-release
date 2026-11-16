"""First-run configuration: private layout, templates, skill copy, preflight."""

from __future__ import annotations

import asyncio
import getpass
import json
import os
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values

from feishu_gateway import config
from feishu_gateway.home import GatewayHome


class BootstrapError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ConfigState:
    home: GatewayHome
    env_exists: bool
    app_id: bool
    app_secret: bool
    preview: bool
    pairing: bool
    ready: bool
    errors: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "home": str(self.home.root),
            "env": str(self.home.env_path),
            "env_exists": self.env_exists,
            "app_id": self.app_id,
            "app_secret": self.app_secret,
            "preview": self.preview,
            "pairing": self.pairing,
            "ready": self.ready,
            "errors": list(self.errors),
        }


def _settings(home: GatewayHome) -> dict[str, str]:
    if not home.env_path.is_file():
        return {}
    return {key: str(value) for key, value in dotenv_values(home.env_path).items() if value is not None}


def inspect(home: GatewayHome | None = None) -> ConfigState:
    home = home or config.HOME
    settings = _settings(home)
    errors: list[str] = []
    env_exists = home.env_path.is_file()
    app_id = bool(settings.get("FEISHU_APP_ID"))
    app_secret = bool(settings.get("FEISHU_APP_SECRET"))
    if not env_exists:
        errors.append(f"missing {home.env_path}")
    if not app_id:
        errors.append("FEISHU_APP_ID is not configured")
    if not app_secret:
        errors.append("FEISHU_APP_SECRET is not configured")
    preview = False
    try:
        preview = config.read_config(home.config_path) is not None
    except config.GatewayConfigError:
        preview = False
    try:
        pairing = bool(config.pairing_settings(settings))
    except config.GatewayConfigError as error:
        pairing = False
        errors.append(str(error))
    local_ready = False
    try:
        from feishu_gateway import ingress_config
        ingress = ingress_config.read_ingress(home.ingress_config_path)
        local_ready = (
            env_exists
            and home.daemon_config_path.is_file()
            and not bool(ingress["feishu"]["enabled"])
        )
    except Exception:
        local_ready = False
    if local_ready:
        return ConfigState(home, True, app_id, app_secret, preview, pairing, True, ())
    if not preview and not pairing:
        errors.append(
            "no preview config and no pairing environment; run gateway-agent init or init --pair"
        )
    ready = env_exists and app_id and app_secret and (preview or pairing)
    return ConfigState(home, env_exists, app_id, app_secret, preview, pairing, ready, tuple(errors))


def _write(path: Path, text: str, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        temporary.unlink(missing_ok=True)


def _upsert_env(text: str, values: dict[str, str]) -> str:
    lines = text.splitlines()
    remaining = dict(values)
    for index, line in enumerate(lines):
        key = line.split("=", 1)[0].strip()
        if key in remaining and not line.lstrip().startswith("#"):
            lines[index] = f"{key}={remaining.pop(key)}"
    if remaining:
        lines.extend(f"{key}={value}" for key, value in remaining.items())
    return "\n".join(lines) + "\n"


def update_env(home: GatewayHome, values: dict[str, str]) -> None:
    """Update selected private environment values without replacing other keys."""
    if not isinstance(values, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in values.items()
    ):
        raise BootstrapError("environment values must be strings")
    current = home.env_path.read_text(encoding="utf-8") if home.env_path.is_file() else ""
    _write(home.env_path, _upsert_env(current, values))


def _prompt(label: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or (default or "")


def _read_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _skill_target(project: Path) -> Path:
    return project / ".agents" / "skills" / "gateway-prompt-preview" / "SKILL.md"


def _install_skill(project: Path) -> Path:
    target = _skill_target(project)
    _write(target, files("feishu_gateway").joinpath(
        "skills", "gateway-prompt-preview", "SKILL.md",
    ).read_text(encoding="utf-8"), 0o600)
    return target


def _json(path: Path, data: object, force: bool) -> None:
    if path.exists() and not force:
        return
    _write(path, json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def initialize(
    home: GatewayHome | None = None, *, non_interactive: bool = False,
    pair: bool = False, force: bool = False, project: Path | None = None,
) -> ConfigState:
    home = home or config.HOME
    home.ensure_dirs()
    project = (project or Path.cwd()).resolve()
    app_id = _read_env("FEISHU_APP_ID")
    app_secret = _read_env("FEISHU_APP_SECRET")
    if not non_interactive:
        app_id = app_id or _prompt("Feishu App ID")
        app_secret = app_secret or getpass.getpass("Feishu App Secret: ").strip()
    if not app_id or not app_secret:
        raise BootstrapError("FEISHU_APP_ID and FEISHU_APP_SECRET are required")

    skill = _install_skill(project)
    env_values = {"FEISHU_APP_ID": app_id, "FEISHU_APP_SECRET": app_secret}
    preview_values: dict[str, str] | None = None
    if pair:
        values = {
            "GATEWAY_PAIR_CODE": "GATEWAY_PAIR_CODE",
            "GATEWAY_TEST_SESSION_ID": "GATEWAY_TEST_SESSION_ID",
            "GATEWAY_CODEX_COMMAND": "GATEWAY_CODEX_COMMAND",
        }
        for key, env_name in values.items():
            value = _read_env(env_name)
            if not value and not non_interactive:
                default = "codex" if key == "GATEWAY_CODEX_COMMAND" else ""
                value = _prompt(env_name, default or None)
            if not value:
                raise BootstrapError(f"{env_name} is required")
            env_values[key] = value
        env_values["GATEWAY_SKILL_PATH"] = str(skill)
    else:
        session = _read_env("GATEWAY_TEST_SESSION_ID")
        chat = _read_env("FEISHU_GATEWAY_CHAT_ID")
        sender = _read_env("FEISHU_GATEWAY_SENDER_ID")
        if not non_interactive:
            session = session or _prompt("Existing Codex session ID")
            chat = chat or _prompt("Allowed Feishu chat ID (oc_...)")
            sender = sender or _prompt("Allowed Feishu sender ID (ou_...)")
        for name, value in (
            ("GATEWAY_TEST_SESSION_ID", session),
            ("FEISHU_GATEWAY_CHAT_ID", chat),
            ("FEISHU_GATEWAY_SENDER_ID", sender),
        ):
            if not value:
                raise BootstrapError(f"{name} is required for preview init")
        try:
            if str(UUID(session)) != session:
                raise ValueError
        except ValueError:
            raise BootstrapError("GATEWAY_TEST_SESSION_ID must be a UUID") from None
        p2p = _read_env("FEISHU_GATEWAY_P2P_CHAT_ID")
        if not non_interactive:
            p2p = p2p or _prompt("Optional personal p2p chat ID (oc_...)", "")
        preview_values = {
            "alias": config.PREVIEW_ALIAS, "session_id": session,
            "skill_path": str(skill), "codex_command": _read_env("GATEWAY_CODEX_COMMAND", "codex"),
            "allowed_chat_id": chat, "allowed_sender_open_id": sender,
            "allowed_p2p_chat_id": p2p or None,
        }

    template = files("feishu_gateway").joinpath("templates", "env.example").read_text(encoding="utf-8")
    base = home.env_path.read_text(encoding="utf-8") if home.env_path.is_file() else template
    _write(home.env_path, _upsert_env(base, env_values), 0o600)
    _json(home.ssh_config_path, {"hosts": []}, force)
    _json(home.dsh_config_path, {"hosts": []}, force)
    _json(home.registry_path, {"version": 1, "hosts": {}}, force)
    if preview_values is not None:
        _json(home.config_path, preview_values, force)
    state = inspect(home)
    if not state.ready:
        raise BootstrapError("; ".join(state.errors))
    return state


_LOCAL_ENV = """# Local Gateway configuration. Feishu is optional.
GATEWAY_ROUTER_BACKEND=auto
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-flash
DEEPSEEK_THINKING=disabled
GATEWAY_ROUTER_FALLBACK=codex
GATEWAY_ROUTER_TIMEOUT_SECONDS=20
"""


@dataclass(frozen=True, slots=True)
class LocalInitResult:
    home: GatewayHome
    skill: Path
    sessions: tuple[dict[str, object], ...] = ()


def initialize_local(
    home: GatewayHome | None = None, *, project: Path | None = None,
    force: bool = False,
) -> LocalInitResult:
    """Create minimal local configuration without requiring or importing Feishu."""
    from feishu_gateway import ingress_config

    home = home or config.HOME
    home.ensure_dirs()
    project = (project or Path.cwd()).resolve()
    skill = _install_skill(project)
    if not home.env_path.exists() or force:
        _write(home.env_path, _LOCAL_ENV, 0o600)
    _json(home.ssh_config_path, {"hosts": []}, force)
    _json(home.dsh_config_path, {"hosts": []}, force)
    _json(home.registry_path, {"version": 1, "hosts": {}}, force)
    _json(home.daemon_config_path, {"log_level": "INFO", "completion_port": 0, "reload_interval": 1.0}, force)
    if not home.ingress_config_path.exists():
        ingress_config.write_ingress(home.ingress_config_path, enabled=False)
    sessions = discover_local(home, project)
    return LocalInitResult(home, skill, tuple(sessions))


def discover_local(home: GatewayHome, project: Path) -> list[dict[str, object]]:
    """Read-only local bootstrap discovery; never writes catalog or registry."""
    from feishu_gateway import config as gateway_config
    from feishu_gateway.discovery import _discover_sessions

    try:
        codex_command = _read_env("GATEWAY_CODEX_COMMAND", "codex")
        scopes = gateway_config.read_ssh_scopes(home.ssh_config_path)
        dsh = tuple(
            item for item in gateway_config.read_dsh_transports(home.dsh_config_path, scopes)
            if item.host == "local"
        )
        snapshot = asyncio.run(_discover_sessions(
            codex_command=codex_command, project_root=project,
            local_roots=(project,), ssh_scopes=(), dsh_transports=dsh,
            hosts=("local",), log_failures=False,
        ))
    except Exception:
        return []
    return [
        {
            "host": candidate.target.host,
            "runtime": candidate.target.runtime,
            "session_id": candidate.target.session_id,
            "title": candidate.title,
            "workspace": str(candidate.cwd),
            "state": "unknown",
        }
        for candidate in snapshot.candidates
    ]
