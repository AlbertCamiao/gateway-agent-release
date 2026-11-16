"""Local Gateway DSH transport enablement."""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path, PurePosixPath

from feishu_gateway import config, host_admin
from feishu_gateway.catalog.hosts import parse_ssh_scopes
from feishu_gateway.executors.dsh import parse_dsh_transports
from feishu_gateway.workspaces.registry import read_document


class LocalDshError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _find(document: dict, host: str) -> dict | None:
    for entry in document.get("hosts", []):
        if isinstance(entry, dict) and str(entry.get("host", "")).casefold() == host.casefold():
            return entry
    return None


def _workspace_paths(entry: dict | None) -> list[str]:
    if entry is None:
        return []
    rows = entry.get("workspaces")
    if not isinstance(rows, list):
        return []
    paths = []
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("path"), str):
            paths.append(row["path"])
    return paths


def ensure_local_dsh(
    host: str,
    profile: str,
    bridge_dir: str,
    *,
    workspaces: tuple[str, ...] = (),
    ssh_path: Path | None = None,
    dsh_path: Path | None = None,
    registry_path: Path | None = None,
) -> dict:
    """Add or update one local DSH transport, preserving other hosts."""
    ssh_path = ssh_path or config.SSH_CONFIG_PATH
    dsh_path = dsh_path or config.DSH_CONFIG_PATH
    registry_path = registry_path or config.REGISTRY_PATH
    try:
        ssh_doc = host_admin._read_doc(ssh_path)
        scopes = parse_ssh_scopes(ssh_doc)
        registry, _ = read_document(registry_path)
    except (ValueError, OSError) as error:
        raise LocalDshError("invalid_config", "Local Gateway host configuration is invalid.") from None

    if host.casefold() not in {scope.transport.host for scope in scopes}:
        raise LocalDshError("unknown_host", f"Host is not configured: {host}")
    if not bridge_dir.startswith("/") or ".." in PurePosixPath(bridge_dir).parts:
        raise LocalDshError("invalid_bridge", "DSH bridge directory must be an absolute canonical path.")

    dsh_doc = host_admin._read_doc(dsh_path) if dsh_path.exists() else {"hosts": []}
    existing = _find(dsh_doc, host)
    selected = list(workspaces)
    if not selected:
        selected = _workspace_paths(existing)
    if not selected:
        selected = [str(path) for path in registry.get(host.casefold(), ())]
    if not selected:
        raise LocalDshError(
            "no_workspace",
            "No registered workspace is available for this host; scan a workspace first.",
        )
    for path in selected:
        if not path.startswith("/") or ".." in PurePosixPath(path).parts:
            raise LocalDshError("no_workspace", f"Invalid workspace path: {path}")

    entry = {
        "host": host,
        "profile": profile,
        "bridge_dir": bridge_dir,
        "workspaces": [{"path": path} for path in dict.fromkeys(selected)],
    }
    if existing is not None and existing.get("profile") == profile and existing.get("bridge_dir") == bridge_dir \
            and _workspace_paths(existing) == [row["path"] for row in entry["workspaces"]]:
        return {"changed": False, "entry": existing, "backup": None}

    with host_admin._lock(dsh_path):
        current = host_admin._read_doc(dsh_path) if dsh_path.exists() else {"hosts": []}
        match = _find(current, host)
        if match is None:
            current["hosts"].append(entry)
        else:
            match.clear()
            match.update(entry)
        try:
            parse_dsh_transports(current, scopes)
        except (ValueError, TypeError) as error:
            raise LocalDshError("invalid_config", "Local DSH configuration would be invalid.") from None
        backup = None
        if dsh_path.exists():
            backup = dsh_path.with_name(dsh_path.name + ".bak." + time.strftime("%Y%m%dT%H%M%S"))
            shutil.copy2(dsh_path, backup)
            os.chmod(backup, 0o600)
        host_admin._write_doc(dsh_path, current)
    return {"changed": True, "entry": entry, "backup": str(backup) if backup else None}


def format_connect(remote: dict, local: dict, restart: str | None) -> str:
    if not remote.get("changed") and not local.get("changed"):
        return "### DSH 连接\n\nBridge is managed and local DSH transport is already enabled; no change was needed."
    after = remote.get("after") or {}
    lines = [
        "### DSH 连接 · 已应用",
        f"- **Host** {after.get('host', '-')}",
        f"- **Remote bridge** {'changed' if remote.get('changed') else 'unchanged'}",
        f"- **Local DSH** {'enabled/updated' if local.get('changed') else 'unchanged'}",
    ]
    if local.get("backup"):
        lines.append(f"- **Local config backup** {local['backup']}")
    if restart == "pending":
        lines.append("- **Gateway** restart pending after this reply")
    elif restart == "restarted":
        lines.append("- **Gateway** restarted")
    elif restart == "manual":
        lines.append("- **Gateway** running manually; restart it yourself")
    elif restart == "not_running":
        lines.append("- **Gateway** not running; configuration applies on next start")
    else:
        lines.append("- **Gateway** restart not required")
    if remote.get("changed"):
        lines.extend((
            "",
            "DSH 正在运行的旧进程尚未加载新 profile patch；请重载 DSH 面板后再验证 socket/list。",
        ))
    return "\n".join(lines)
