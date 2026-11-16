"""Read-only DSH bridge status and connection plan."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from feishu_gateway import config
from feishu_gateway.executors.codex_remote import SSHReadTransport
from feishu_gateway.workspaces.target import host_for


MANIFEST_PATH = files("feishu_gateway.bridge").joinpath("assets", "manifest.json")
_PROFILE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_REMOTE = r'''
import hashlib
import json
import os
import re
import stat
import subprocess
from pathlib import Path

home = Path.home()
profile = __PROFILE__
bridge = home / ".dsh" / "gateway-bridge"
profile_dir = home / ".dsh" / "profiles" / profile
patch = profile_dir / "cordis.patch.yml"


def info(path):
    try:
        entry = path.lstat()
    except OSError:
        return None
    return {
        "mode": format(stat.S_IMODE(entry.st_mode), "04o"),
        "uid": entry.st_uid,
        "is_file": stat.S_ISREG(entry.st_mode),
        "is_dir": stat.S_ISDIR(entry.st_mode),
        "is_socket": stat.S_ISSOCK(entry.st_mode),
        "size": entry.st_size,
    }


def read_text(path, limit):
    try:
        entry = path.lstat()
        if not stat.S_ISREG(entry.st_mode) or entry.st_size > limit:
            return None
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None


plugin = info(bridge / "plugin.mjs")
token = info(bridge / "bridge-token")
scope = info(bridge / "bridge-workspaces.json")
socket = info(bridge / "bridge.sock")
patch_info = info(patch)
bridge_dir = info(bridge)
profile_dir_info = info(profile_dir)
plugin_sha = None
if plugin is not None and plugin["is_file"]:
    try:
        plugin_sha = hashlib.sha256((bridge / "plugin.mjs").read_bytes()).hexdigest()
    except OSError:
        plugin_sha = None

scope_roots = None
scope_valid = False
scope_text = read_text(bridge / "bridge-workspaces.json", 32 * 1024)
if scope_text is not None:
    try:
        document = json.loads(scope_text)
        roots = document.get("roots") if isinstance(document, dict) else None
        if isinstance(roots, list) and len(roots) <= 32:
            parsed = []
            for root in roots:
                if not isinstance(root, dict):
                    raise ValueError
                path = root.get("path")
                include = root.get("includeDescendants", False)
                if not isinstance(path, str) or not path or not path.startswith("/") \
                        or path.startswith("//") or path.endswith("/") or ".." in path.split("/") \
                        or not isinstance(include, bool):
                    raise ValueError
                parsed.append({"path": path, "includeDescendants": include})
            scope_roots = parsed
            scope_valid = True
    except (ValueError, TypeError, json.JSONDecodeError):
        scope_valid = False

patch_text = read_text(patch, 256 * 1024)
has_id = bool(patch_text and re.search(r"(?m)^\s*-\s*id:\s*['\"]?feishuGatewayBridge['\"]?\s*$", patch_text))
has_marker = bool(patch_text and ">>> feishu-gateway-bridge managed >>>" in patch_text)
bridge_path = None
if patch_text:
    match = re.search(r"feishuGatewayBridge[\s\S]{0,240}?name:\s*['\"]([^'\"]+)['\"]", patch_text)
    if match:
        bridge_path = match.group(1)

ps = subprocess.run(["ps", "-eo", "args="], capture_output=True, text=True)
running = any(
    "dsh" in line and " web" in line and "ps -eo" not in line
    for line in ps.stdout.splitlines()
)

print(json.dumps({
    "home": str(home),
    "profile": profile,
    "profile_dir": str(profile_dir),
    "profile_exists": profile_dir_info is not None and profile_dir_info["is_dir"],
    "bridge_dir": str(bridge),
    "bridge_dir_mode": bridge_dir["mode"] if bridge_dir else None,
    "plugin": plugin,
    "plugin_sha256": plugin_sha,
    "token": token,
    "scope": scope,
    "scope_roots": scope_roots,
    "scope_valid": scope_valid,
    "patch": patch_info,
    "patch_has_id": has_id,
    "patch_has_marker": has_marker,
    "patch_bridge_path": bridge_path,
    "socket": socket,
    "dsh_running": running,
}, ensure_ascii=False))
'''


class BridgeStatusError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class LocalDsh:
    configured: bool
    profile: str | None
    bridge_dir: str | None


def _manifest() -> dict:
    try:
        data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise BridgeStatusError("manifest_unavailable", "Bridge manifest is unavailable.") from None
    if not isinstance(data, dict) or not isinstance(data.get("sha256"), str):
        raise BridgeStatusError("manifest_unavailable", "Bridge manifest is invalid.")
    return data


def _resolve(host_phrase: str) -> tuple[str, SSHReadTransport]:
    try:
        scopes = config.read_ssh_scopes(config.SSH_CONFIG_PATH)
        host, transport = host_for(host_phrase, scopes)
    except (config.GatewayConfigError, ValueError) as error:
        raise BridgeStatusError("unknown_host", str(error)) from None
    if transport is None:
        raise BridgeStatusError("local_unsupported", "DSH bridge status is only supported for SSH hosts.")
    return host, transport


def _local_dsh(host: str) -> LocalDsh:
    try:
        document = config.read_private_json(config.DSH_CONFIG_PATH)
    except (OSError, ValueError):
        return LocalDsh(False, None, None)
    if not isinstance(document, dict) or not isinstance(document.get("hosts"), list):
        return LocalDsh(False, None, None)
    for entry in document["hosts"]:
        if not isinstance(entry, dict) or str(entry.get("host", "")).casefold() != host.casefold():
            continue
        return LocalDsh(
            True,
            entry.get("profile") if isinstance(entry.get("profile"), str) else None,
            entry.get("bridge_dir") if isinstance(entry.get("bridge_dir"), str) else None,
        )
    return LocalDsh(False, None, None)


def _remote(transport: SSHReadTransport, profile: str) -> dict:
    script = _REMOTE.replace("__PROFILE__", json.dumps(profile))
    alias = transport.ssh_alias or transport.host
    command = [
        "/usr/bin/ssh", "-T",
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", "ControlMaster=no", "-o", "ControlPath=none",
        "--", alias, "python3 -I -c " + shlex.quote(script),
    ]
    environment = {
        name: os.environ[name] for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
        if name in os.environ
    }
    try:
        result = subprocess.run(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=25, check=False, env=environment,
        )
    except (OSError, subprocess.SubprocessError):
        raise BridgeStatusError("ssh_unavailable", f"{alias}: SSH status check failed.") from None
    if result.returncode != 0:
        detail = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "SSH failed"
        raise BridgeStatusError("ssh_unavailable", f"{alias}: {detail[:200]}")
    try:
        data = json.loads(result.stdout)
    except ValueError:
        raise BridgeStatusError("remote_protocol", f"{alias}: invalid status response.") from None
    if not isinstance(data, dict) or not isinstance(data.get("home"), str):
        raise BridgeStatusError("remote_protocol", f"{alias}: invalid status response.")
    return data


def classify(snapshot: dict, manifest: dict, local: LocalDsh) -> dict:
    profile = snapshot["profile"]
    plugin = snapshot.get("plugin")
    token = snapshot.get("token")
    scope = snapshot.get("scope")
    patch = snapshot.get("patch")
    bridge_dir_mode = snapshot.get("bridge_dir_mode")
    scope_roots = snapshot.get("scope_roots")
    scope_valid = bool(snapshot.get("scope_valid"))
    plugin_sha = snapshot.get("plugin_sha256")
    has_id = bool(snapshot.get("patch_has_id"))
    has_marker = bool(snapshot.get("patch_has_marker"))
    patch_bridge_path = snapshot.get("patch_bridge_path")
    socket = snapshot.get("socket")
    dsh_running = bool(snapshot.get("dsh_running"))

    issues: list[str] = []
    if not snapshot.get("profile_exists"):
        state = "profile_missing"
        issues.append(f"DSH profile {profile} is missing.")
    elif not snapshot.get("bridge_dir") or plugin is None:
        state = "missing"
        issues.append("Bridge plugin is missing.")
    elif not token or not scope or not scope_valid:
        state = "incomplete"
        if not token:
            issues.append("Bridge token is missing.")
        if not scope:
            issues.append("Bridge scope file is missing.")
        elif not scope_valid:
            issues.append("Bridge scope is invalid.")
    elif plugin_sha != manifest["sha256"]:
        state = "outdated"
        issues.append("Bridge plugin hash does not match the canonical manifest.")
    elif has_id and has_marker:
        state = "managed"
    else:
        state = "unmanaged"
        if not has_id:
            issues.append("Profile patch has no feishuGatewayBridge entry.")
        elif not has_marker:
            issues.append("Profile patch entry is unmanaged (no managed marker).")

    if bridge_dir_mode != "0700":
        issues.append(f"Bridge directory mode is {bridge_dir_mode or 'missing'}, expected 0700.")
    if plugin and plugin.get("mode") != "0600":
        issues.append(f"Plugin mode is {plugin.get('mode')}, expected 0600.")
    if token and token.get("mode") != "0600":
        issues.append(f"Token mode is {token.get('mode')}, expected 0600.")
    if scope and scope.get("mode") != "0600":
        issues.append(f"Scope mode is {scope.get('mode')}, expected 0600.")
    if patch and patch.get("mode") != "0600":
        issues.append(f"Profile patch mode is {patch.get('mode')}, expected 0600.")
    if has_id and patch_bridge_path and patch_bridge_path != str(Path(snapshot["bridge_dir"]) / "plugin.mjs"):
        issues.append("Profile patch bridge path differs from the configured bridge directory.")

    patch_path_mismatch = bool(
        has_id and patch_bridge_path
        and patch_bridge_path != str(Path(snapshot["bridge_dir"]) / "plugin.mjs")
    )
    mode_bad = (
        bridge_dir_mode != "0700"
        or bool(plugin and plugin.get("mode") != "0600")
        or bool(token and token.get("mode") != "0600")
        or bool(scope and scope.get("mode") != "0600")
        or bool(patch and patch.get("mode") != "0600")
    )
    bridge_changes = state in {"missing", "incomplete", "outdated", "unmanaged"} or patch_path_mismatch
    local_enable = not local.configured or local.profile != profile or local.bridge_dir != snapshot["bridge_dir"]
    actions: list[str] = []
    if state == "profile_missing":
        actions.append("blocked:profile_missing")
    else:
        if state in {"missing", "outdated"}:
            actions.append("write_plugin")
        if state == "missing" or not token:
            actions.append("generate_token")
        if state in {"missing", "incomplete"}:
            actions.append("initialize_scope")
        if not has_id:
            actions.append("insert_patch")
        elif not has_marker or patch_path_mismatch:
            actions.append("adopt_patch")
        if mode_bad:
            actions.append("fix_permissions")
        if local_enable:
            actions.append("enable_local_dsh")
            actions.append("restart_gateway")
        if bridge_changes and dsh_running:
            actions.append("reload_dsh")
        elif bridge_changes and not dsh_running:
            actions.append("start_dsh")

    needs_gateway_restart = "restart_gateway" in actions
    return {
        "host": snapshot.get("host"),
        "profile": profile,
        "home": snapshot["home"],
        "state": state,
        "can_apply": state != "profile_missing",
        "issues": issues,
        "actions": actions,
        "bridge_changes": bridge_changes,
        "needs_gateway_restart": needs_gateway_restart,
        "needs_dsh_reload": "reload_dsh" in actions,
        "needs_dsh_start": "start_dsh" in actions,
        "preserve_token": bool(token),
        "preserve_scope": bool(scope_roots),
        "plugin": {
            "present": plugin is not None,
            "sha256": plugin_sha,
            "matches_manifest": plugin_sha == manifest["sha256"],
            "size": plugin.get("size") if plugin else None,
        },
        "token": {"present": token is not None, "mode": token.get("mode") if token else None},
        "scope": {
            "present": scope is not None,
            "valid": scope_valid,
            "roots": scope_roots or [],
            "mode": scope.get("mode") if scope else None,
        },
        "patch": {
            "present": patch is not None,
            "has_id": has_id,
            "managed_marker": has_marker,
            "bridge_path": patch_bridge_path,
            "mode": patch.get("mode") if patch else None,
        },
        "socket": {
            "present": socket is not None,
            "is_socket": bool(socket and socket.get("is_socket")),
            "mode": socket.get("mode") if socket else None,
        },
        "dsh_running": dsh_running,
        "local_dsh": {
            "configured": local.configured,
            "profile": local.profile,
            "bridge_dir": local.bridge_dir,
            "matches": local.configured and local.profile == profile and local.bridge_dir == snapshot["bridge_dir"],
        },
        "manifest": {
            "version": manifest.get("version"),
            "sha256": manifest.get("sha256"),
            "tested_with_dsh": manifest.get("tested_with_dsh"),
        },
    }


def status(host_phrase: str, profile: str | None = None) -> dict:
    host, transport = _resolve(host_phrase)
    local = _local_dsh(host)
    selected = profile or local.profile or "web"
    if not _PROFILE.fullmatch(selected):
        raise BridgeStatusError("invalid_profile", "Invalid DSH profile name.")
    snapshot = _remote(transport, selected)
    snapshot["host"] = host
    return classify(snapshot, _manifest(), local)


_ACTION_LABELS = {
    "write_plugin": "写入/更新 bridge plugin",
    "generate_token": "生成 bridge token（保留现有 token）",
    "initialize_scope": "初始化/修复 workspace scope",
    "insert_patch": "插入 profile patch",
    "adopt_patch": "接管现有 profile patch（加 managed 标记）",
    "fix_permissions": "修复 bridge 文件权限",
    "enable_local_dsh": "启用本地 DSH transport",
    "restart_gateway": "重启 Gateway",
    "reload_dsh": "重载 DSH Host",
    "start_dsh": "下次启动 DSH Host 时加载",
    "blocked:profile_missing": "阻塞：DSH profile 不存在",
}
