"""Remote bridge adoption/install for the DSH Connect pipeline."""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
from importlib.resources import files

from feishu_gateway.bridge.status import _resolve, status as bridge_status
from feishu_gateway.executors.codex_remote import SSHReadTransport


ASSET = files("feishu_gateway.bridge").joinpath("assets", "plugin.mjs")
MARKER_START = "# >>> feishu-gateway-bridge managed >>>"
MARKER_END = "# <<< feishu-gateway-bridge managed <<<"

_APPLY = r'''
import base64
import json
import os
import re
import secrets
import shutil
import stat
import sys
import time
from pathlib import Path

payload = json.load(sys.stdin)
home = Path.home()
profile = payload["profile"]
bridge = home / ".dsh" / "gateway-bridge"
profile_dir = home / ".dsh" / "profiles" / profile
patch = profile_dir / "cordis.patch.yml"
plugin_path = bridge / "plugin.mjs"
marker_start = payload["marker_start"]
marker_end = payload["marker_end"]
stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())

if not profile_dir.is_dir():
    raise SystemExit("profile_missing")
bridge.mkdir(mode=0o700, parents=True, exist_ok=True)
os.chmod(bridge, 0o700)
backups = []


def backup(path):
    if path.exists():
        target = path.with_name(path.name + ".bak." + stamp)
        shutil.copy2(path, target)
        os.chmod(target, 0o600)
        backups.append(str(target))


def atomic(path, data, mode=0o600):
    temporary = path.with_name(path.name + ".tmp." + stamp + "." + str(os.getpid()))
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def manage_patch(text, plugin):
    lines = text.splitlines()
    if any(marker_start in line for line in lines) and any(marker_end in line for line in lines):
        start = next(i for i, line in enumerate(lines) if marker_start in line)
        end = next(i for i, line in enumerate(lines) if marker_end in line)
        if end <= start:
            raise SystemExit("invalid managed marker")
        lines = lines[:start] + lines[end + 1:]

    id_pattern = re.compile(r"^\s*-\s*id:\s*['\"]?feishuGatewayBridge['\"]?\s*$")
    id_index = next((i for i, line in enumerate(lines) if id_pattern.match(line)), None)
    if id_index is not None:
        block_start = next(
            (i for i in range(id_index, max(-1, id_index - 16), -1)
             if re.match(r"^\s*-\s*insert:\s*$", lines[i])),
            id_index,
        )
        block_end = len(lines)
        for i in range(id_index + 1, len(lines)):
            if re.match(r"^-\s", lines[i]):
                block_end = i
                break
        block = lines[block_start:block_end]
        name_index = next(
            (i for i, line in enumerate(block) if re.match(r"^\s*name:\s*", line)),
            None,
        )
        if name_index is None:
            block.insert(1, "      name: " + repr(str(plugin)))
        else:
            indent = re.match(r"^(\s*)", block[name_index]).group(1)
            block[name_index] = indent + "name: " + repr(str(plugin))
        lines = lines[:block_start] + [marker_start, *block, marker_end] + lines[block_end:]
    else:
        lines.extend([
            "", marker_start,
            "- insert:",
            "    - id: feishuGatewayBridge",
            "      name: " + repr(str(plugin)),
            marker_end,
        ])
    return "\n".join(lines).rstrip() + "\n"


if payload.get("write_plugin"):
    backup(plugin_path)
    atomic(plugin_path, base64.b64decode(payload["plugin_b64"]))

token = bridge / "bridge-token"
token_created = False
if not token.exists():
    atomic(token, secrets.token_urlsafe(32).encode())
    token_created = True
else:
    os.chmod(token, 0o600)

scope = bridge / "bridge-workspaces.json"
scope_created = False
if not scope.exists():
    atomic(scope, (json.dumps({"roots": []}, ensure_ascii=False) + "\n").encode())
    scope_created = True
else:
    os.chmod(scope, 0o600)

old_patch = patch.read_text(encoding="utf-8") if patch.exists() else ""
new_patch = manage_patch(old_patch, plugin_path)
patch_changed = new_patch != old_patch
if patch_changed:
    backup(patch)
    atomic(patch, new_patch.encode())
else:
    os.chmod(patch, 0o600)

os.chmod(bridge, 0o700)
print(json.dumps({
    "backups": backups,
    "token_created": token_created,
    "scope_created": scope_created,
    "patch_changed": patch_changed,
    "plugin_written": bool(payload.get("write_plugin")),
}))
'''


class BridgeApplyError(ValueError):
    def __init__(self, code: str, message: str, *, backups: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.code = code
        self.backups = backups


def apply_bridge_only(host_phrase: str, profile: str | None = None) -> dict:
    before = bridge_status(host_phrase, profile)
    if not before["can_apply"]:
        raise BridgeApplyError("profile_missing", "DSH profile is missing; cannot apply the bridge.")
    if before["state"] == "managed" and not before["actions"]:
        return {"changed": False, "before": before, "after": before, "remote": None}

    _, transport = _resolve(host_phrase)
    payload = {
        "profile": before["profile"],
        "plugin_b64": base64.b64encode(ASSET.read_bytes()).decode("ascii"),
        "write_plugin": not before["plugin"]["matches_manifest"],
        "marker_start": MARKER_START,
        "marker_end": MARKER_END,
    }
    remote = _remote_apply(transport, payload)
    after = bridge_status(host_phrase, before["profile"])
    if after["state"] != "managed":
        raise BridgeApplyError(
            "adopt_incomplete",
            f"Bridge apply did not reach managed state (got {after['state']}).",
            backups=tuple(remote.get("backups", ())),
        )
    return {"changed": True, "before": before, "after": after, "remote": remote}


def _remote_apply(transport: SSHReadTransport, payload: dict) -> dict:
    alias = transport.ssh_alias or transport.host
    command = [
        "/usr/bin/ssh", "-T",
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", "ControlMaster=no", "-o", "ControlPath=none",
        "--", alias, "python3 -I -c " + shlex.quote(_APPLY),
    ]
    environment = {
        name: os.environ[name] for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
        if name in os.environ
    }
    try:
        result = subprocess.run(
            command, input=json.dumps(payload), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=45, check=False, env=environment,
        )
    except (OSError, subprocess.SubprocessError):
        raise BridgeApplyError("ssh_unavailable", f"{alias}: SSH apply failed.") from None
    if result.returncode != 0:
        detail = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "SSH apply failed"
        raise BridgeApplyError("remote_apply_failed", f"{alias}: {detail[:300]}")
    try:
        data = json.loads(result.stdout)
    except ValueError:
        raise BridgeApplyError("remote_protocol", f"{alias}: invalid apply response.") from None
    if not isinstance(data, dict) or not isinstance(data.get("backups"), list):
        raise BridgeApplyError("remote_protocol", f"{alias}: invalid apply response.")
    return data


def format_apply(result: dict) -> str:
    if not result.get("changed"):
        return "### DSH 连接\n\nBridge is already managed; no change was needed."
    remote = result.get("remote") or {}
    after = result["after"]
    lines = [
        "### DSH 连接 · 远端已应用",
        f"- **Host** {after['host']}",
        f"- **State** `{after['state']}`",
        f"- **Plugin written** {'yes' if remote.get('plugin_written') else 'no (canonical hash already matched)'}",
        f"- **Token** {'created' if remote.get('token_created') else 'preserved'}",
        f"- **Scope** {'initialized' if remote.get('scope_created') else 'preserved'}",
        f"- **Profile patch changed** {'yes' if remote.get('patch_changed') else 'no'}",
    ]
    backups = remote.get("backups") or []
    if backups:
        lines.append("- **Backups**")
        lines.extend(f"  - {path}" for path in backups)
    lines.extend((
        "",
        "远端 bridge 已写入；DSH 正在运行的旧进程尚未加载新 profile patch。",
        "请重载 DSH 面板后再验证 socket/list。",
    ))
    return "\n".join(lines)
