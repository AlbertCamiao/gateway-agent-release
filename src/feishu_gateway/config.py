"""Configuration, project paths, and private JSON loading."""

from __future__ import annotations

import json
import os
import shutil
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping
from uuid import UUID

from feishu_gateway.catalog.hosts import SSHScope, parse_ssh_scopes
from feishu_gateway.executors.codex_app_server import SKILL_NAME
from feishu_gateway.executors.dsh import DSHReadTransport, parse_dsh_transports
from feishu_gateway.home import GatewayHome


HOME = GatewayHome.discover()
PROJECT_ROOT = HOME.root
ENV_PATH = HOME.env_path
CONFIG_PATH = HOME.config_path
SSH_CONFIG_PATH = HOME.ssh_config_path
DSH_CONFIG_PATH = HOME.dsh_config_path
LISTENER_LOCK_PATH = HOME.listener_lock_path
REGISTRY_PATH = HOME.registry_path
COMPLETION_STATE_PATH = HOME.completion_state_path
COMPLETION_PORT = 39021


PREVIEW_ALIAS = "预览测试"


PAIR_ENV = (
    "GATEWAY_PAIR_CODE",
    "GATEWAY_TEST_SESSION_ID",
    "GATEWAY_SKILL_PATH",
    "GATEWAY_CODEX_COMMAND",
)


class GatewayConfigError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PreviewConfig:
    alias: str
    session_id: str
    skill_path: str
    codex_command: str
    allowed_chat_id: str
    allowed_sender_open_id: str
    allowed_p2p_chat_id: str | None = None


def valid_id(value: str, prefix: str) -> bool:
    return isinstance(value, str) and value.startswith(prefix) and len(value) > len(prefix) and not any(
        character.isspace() for character in value
    )


def check_runtime(session_id: str, skill_path: str, codex_command: str) -> None:
    try:
        if str(UUID(session_id)) != session_id:
            raise ValueError
        skill = Path(skill_path)
        if not skill.is_absolute() or skill.name != "SKILL.md" or not skill.is_file():
            raise ValueError
        if not shutil.which(codex_command):
            raise ValueError
    except (ValueError, OSError) as error:
        raise GatewayConfigError("预览测试会话、技能或 Codex 命令无效。") from error


def read_private_json(path: Path) -> dict | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError:
        raise GatewayConfigError(f"无法读取本机配置 {path.name}。") from None

    try:
        with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
            entry = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(entry.st_mode)
                or entry.st_uid != os.getuid()
                or stat.S_IMODE(entry.st_mode) != 0o600
            ):
                raise GatewayConfigError(f"配置 {path.name} 须由当前用户持有，权限为 0600。")
            data = json.load(stream)
            if not isinstance(data, dict):
                raise ValueError
            return data
    except (ValueError, OSError):
        raise GatewayConfigError(f"本机配置 {path.name} 无效。") from None


def read_config(path: Path) -> PreviewConfig | None:
    data = read_private_json(path)
    if data is None:
        return None
    try:
        config = PreviewConfig(**data)
        values = asdict(config)
        values.pop("allowed_p2p_chat_id")
        if not all(isinstance(value, str) for value in values.values()):
            raise ValueError
        if (
            config.alias != PREVIEW_ALIAS
            or not valid_id(config.allowed_chat_id, "oc_")
            or not valid_id(config.allowed_sender_open_id, "ou_")
            or (
                config.allowed_p2p_chat_id is not None
                and not valid_id(config.allowed_p2p_chat_id, "oc_")
            )
        ):
            raise ValueError
        check_runtime(config.session_id, config.skill_path, config.codex_command)
        return config
    except GatewayConfigError:
        raise
    except (TypeError, ValueError, OSError):
        raise GatewayConfigError("本机预览配置无效。") from None


def read_ssh_scopes(path: Path) -> tuple[SSHScope, ...]:
    try:
        return parse_ssh_scopes(read_private_json(path))
    except ValueError:
        raise GatewayConfigError("远端会话配置无效。") from None


def read_dsh_transports(
    path: Path, ssh_scopes: tuple[SSHScope, ...],
) -> tuple[DSHReadTransport, ...]:
    try:
        return parse_dsh_transports(read_private_json(path), ssh_scopes)
    except ValueError:
        raise GatewayConfigError("DSH 会话配置无效。") from None


def pairing_settings(settings: Mapping[str, str] | None = None) -> tuple[str, str, str, str] | None:
    source = settings if settings is not None else os.environ
    values = tuple(source.get(name, "") for name in PAIR_ENV)
    if not any(values):
        return None
    if not all(values) or any(character.isspace() for character in values[0]):
        raise GatewayConfigError("一次性绑定配置不完整或无效。")
    code, session_id, skill_path, codex_command = values
    check_runtime(session_id, skill_path, codex_command)
    return code, session_id, skill_path, codex_command
