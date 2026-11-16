"""Install the canonical Codex Stop hook on a configured remote host."""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
from pathlib import Path

from feishu_gateway import config
from feishu_gateway.execution.connect_ports import ConnectError, HookResult
from feishu_gateway.workspaces.registry import WorkspaceRegistryError
from feishu_gateway.workspaces.target import host_for

_ASSET = Path(__file__).with_name("assets") / "codex-stop.py"
_REMOTE = r'''
import base64, json, os, pathlib, shutil, sys, time
data = json.load(sys.stdin)
home = pathlib.Path.home()
hooks = home / ".codex" / "hooks"
hooks.mkdir(mode=0o700, parents=True, exist_ok=True)
os.chmod(hooks, 0o700)
script = hooks / "feishu-stop.py"
tmp = hooks / ".feishu-stop.tmp"
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "wb") as stream:
    stream.write(base64.b64decode(data["script"]))
os.replace(tmp, script)
os.chmod(script, 0o600)
config_path = home / ".codex" / "hooks.json"
existing = {}
if config_path.exists():
    backup = config_path.with_name(config_path.name + ".bak." + time.strftime("%Y%m%dT%H%M%S"))
    shutil.copyfile(config_path, backup)
    try:
        existing = json.loads(config_path.read_text())
    except Exception:
        existing = {}
if not isinstance(existing, dict):
    existing = {}
hooks_obj = existing.setdefault("hooks", {})
if not isinstance(hooks_obj, dict):
    hooks_obj = {}
    existing["hooks"] = hooks_obj
entries = hooks_obj.setdefault("Stop", [])
if not isinstance(entries, list):
    entries = []
    hooks_obj["Stop"] = entries
command = "python3 " + str(script)
if not any(
    isinstance(item, dict) and any(
        isinstance(handler, dict) and handler.get("command") == command
        for handler in (item.get("hooks") or [])
    ) for item in entries
):
    entries.append({"hooks": [{"type": "command", "command": command, "timeout": 5}]})
tmp = config_path.with_name(config_path.name + ".tmp")
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as stream:
    json.dump(existing, stream, ensure_ascii=False, indent=2)
    stream.flush(); os.fsync(stream.fileno())
os.replace(tmp, config_path)
os.chmod(config_path, 0o600)
print(json.dumps({"ok": True}))
'''


class CodexHookInstallerAdapter:
    async def install(self, host: str) -> HookResult:
        try:
            scopes = config.read_ssh_scopes(config.SSH_CONFIG_PATH)
            resolved, transport = host_for(host, scopes)
        except (WorkspaceRegistryError, config.GatewayConfigError, OSError):
            raise ConnectError("host_not_configured", "主机未配置或不可唯一解析。") from None
        if transport is None or transport.control_path is None:
            raise ConnectError("invalid_args", "远端连接不能使用 local。")
        if not transport.ensure_control_master():
            raise ConnectError("upstream_error", "SSH ControlMaster 不可用。")
        payload = {"script": base64.b64encode(_ASSET.read_bytes()).decode()}
        encoded = base64.b64encode(_REMOTE.encode()).decode()
        command = transport._ssh_command(
            "exec python3 -I -c " + shlex.quote(
                f"import base64; exec(base64.b64decode({encoded!r}))"
            )
        )
        env = {
            name: os.environ[name] for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
            if name in os.environ
        }
        result = subprocess.run(
            command, input=json.dumps(payload), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False, env=env,
        )
        try:
            ok = json.loads(result.stdout or "{}").get("ok") is True
        except (ValueError, AttributeError):
            ok = False
        if result.returncode != 0 or not ok:
            raise ConnectError("upstream_error", "Codex Hook 安装失败。")
        return HookResult(resolved, True, True, ("Codex: review and trust the Stop hook once.",))


def install_local_hook() -> None:
    """Install the canonical Stop hook and config on the local machine."""
    hooks = Path.home() / ".codex" / "hooks"
    hooks.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(hooks, 0o700)
    script = hooks / "feishu-stop.py"
    temporary = hooks / ".feishu-stop.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(_ASSET.read_bytes())
    os.replace(temporary, script)
    os.chmod(script, 0o600)

    config_path = Path.home() / ".codex" / "hooks.json"
    existing: dict = {}
    if config_path.exists():
        try:
            loaded = json.loads(config_path.read_text())
            existing = loaded if isinstance(loaded, dict) else {}
        except Exception:
            existing = {}
    hooks_obj = existing.setdefault("hooks", {})
    if not isinstance(hooks_obj, dict):
        hooks_obj = {}
        existing["hooks"] = hooks_obj
    entries = hooks_obj.setdefault("Stop", [])
    if not isinstance(entries, list):
        entries = []
        hooks_obj["Stop"] = entries
    command = "python3 " + str(script)
    if not any(
        isinstance(item, dict) and any(
            isinstance(handler, dict) and handler.get("command") == command
            for handler in (item.get("hooks") or [])
        ) for item in entries
    ):
        entries.append({"hooks": [{"type": "command", "command": command, "timeout": 5}]})
    config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = config_path.with_name(config_path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(existing, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, config_path)
    os.chmod(config_path, 0o600)
