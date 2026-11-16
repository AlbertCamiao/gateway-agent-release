"""Local SSH host administration for the Gateway CLI."""

from __future__ import annotations

import fcntl
import json
import os
import re
import shlex
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile

from feishu_gateway import config
from feishu_gateway.catalog.hosts import parse_ssh_scopes
from feishu_gateway.executors.dsh import parse_dsh_transports
from feishu_gateway.workspaces.registry import read_document, registry_lock, write_registry


_SSH = "/usr/bin/ssh"
_SSH_BASE = (
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=5",
    "-o", "ControlMaster=no",
    "-o", "ControlPath=none",
)
_PROFILE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_PROBE = r"""
import json
import shutil
from pathlib import Path

home = Path.home()
workspace = home / "workspace"
bridge = home / ".dsh" / "gateway-bridge"
print(json.dumps({
    "home": str(home),
    "workspace": str(workspace) if workspace.is_dir() else None,
    "codex": shutil.which("codex"),
    "bridge": str(bridge),
    "bridge_exists": bridge.is_dir(),
    "token": (bridge / "bridge-token").is_file(),
    "scope": (bridge / "bridge-workspaces.json").is_file(),
}))
"""


class HostAdminError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class SSHCandidate:
    alias: str
    hostname: str
    user: str
    port: int
    identity_files: tuple[str, ...]


def _ssh_environment() -> dict[str, str]:
    return {
        name: os.environ[name]
        for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
        if name in os.environ
    }


def _ssh_run(args: list[str], *, timeout: float, payload: bytes | None = None):
    try:
        return subprocess.run(
            [_SSH, *args],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=_ssh_environment(),
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def _ssh_identity(alias: str, *, timeout: float = 5) -> SSHCandidate:
    result = _ssh_run([*_SSH_BASE, "-G", "--", alias], timeout=timeout)
    if result is None or result.returncode != 0:
        raise HostAdminError("ssh_unknown", f"SSH host {alias!r} cannot be resolved.")
    fields: dict[str, str] = {}
    identity_files: list[str] = []
    for line in result.stdout.decode("utf-8", "replace").splitlines():
        key, _, value = line.partition(" ")
        if key in {"hostname", "user", "port"} and key not in fields:
            fields[key] = value.strip()
        elif key == "identityfile":
            identity_files.append(value.strip())
    try:
        return SSHCandidate(
            alias, fields["hostname"], fields["user"], int(fields["port"]),
            tuple(identity_files),
        )
    except (KeyError, ValueError):
        raise HostAdminError("ssh_unknown", f"SSH host {alias!r} has an invalid identity.") from None


def inspect_ssh_host(alias: str, *, timeout: float = 5) -> SSHCandidate:
    candidate = _ssh_identity(alias, timeout=timeout)
    result = _ssh_run([*_SSH_BASE, "--", alias, "true"], timeout=timeout)
    if result is None or result.returncode != 0:
        raise HostAdminError(
            "ssh_unavailable",
            f"{alias} cannot be reached with key authentication (BatchMode).",
        )
    return candidate


def probe_remote(alias: str, *, timeout: float = 10) -> dict:
    result = _ssh_run(
        [*_SSH_BASE, "--", alias, "python3 -I -c " + shlex.quote(_PROBE)], timeout=timeout,
    )
    if result is None or result.returncode != 0:
        raise HostAdminError(
            "remote_probe_failed",
            f"{alias}: python3 or the remote probe is unavailable.",
        )
    try:
        data = json.loads(result.stdout.decode("utf-8"))
    except (UnicodeError, ValueError):
        raise HostAdminError("remote_probe_failed", f"{alias}: remote probe returned invalid data.") from None
    if not isinstance(data, dict) or not isinstance(data.get("home"), str):
        raise HostAdminError("remote_probe_failed", f"{alias}: remote probe returned invalid data.")
    return data


def remote_directory(alias: str, path: str, *, timeout: float = 8) -> bool:
    command = f"test -d {shlex.quote(path)}"
    result = _ssh_run([*_SSH_BASE, "--", alias, command], timeout=timeout)
    return result is not None and result.returncode == 0


def remote_file(alias: str, path: str, *, timeout: float = 8) -> bool:
    command = f"test -f {shlex.quote(path)}"
    result = _ssh_run([*_SSH_BASE, "--", alias, command], timeout=timeout)
    return result is not None and result.returncode == 0


def _ssh_config_hosts(path: Path) -> tuple[str, ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    names: list[str] = []
    seen: set[str] = set()
    for line in lines:
        line = line.split("#", 1)[0].strip()
        if not line or line.split(maxsplit=1)[0].lower() != "host":
            continue
        for name in line.split()[1:]:
            if not name or any(char in name for char in "*?![") or name in seen:
                continue
            seen.add(name)
            names.append(name)
    return tuple(names)


def candidate_hosts(
    *, ssh_config: Path | None = None, configured: set[str] | None = None,
    timeout: float = 5,
) -> tuple[dict, ...]:
    configured = configured or set()
    path = ssh_config or Path.home() / ".ssh" / "config"
    candidates = []
    for alias in _ssh_config_hosts(path):
        if alias.casefold() in configured:
            continue
        try:
            candidate = inspect_ssh_host(alias, timeout=timeout)
        except HostAdminError:
            continue
        candidates.append({
            "alias": candidate.alias,
            "hostname": candidate.hostname,
            "user": candidate.user,
            "port": candidate.port,
        })
    return tuple(candidates)


def _read_doc(path: Path) -> dict:
    data = config.read_private_json(path)
    if data is None:
        return {"hosts": []}
    if not isinstance(data, dict) or not isinstance(data.get("hosts"), list):
        raise HostAdminError("invalid_config", f"{path.name} has an invalid format.")
    return data


def _write_doc(path: Path, data: dict) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2).encode() + b"\n"
    with NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


@contextmanager
def _lock(path: Path):
    lock_path = path.with_name(path.name + ".lock")
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    except OSError:
        raise HostAdminError("invalid_config", "Could not open the host configuration lock.") from None
    try:
        info = os.fstat(descriptor)
        if not os.path.isfile(lock_path) or info.st_uid != os.getuid():
            raise HostAdminError("invalid_config", "Host configuration lock is not private.")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _validate(ssh_doc: dict, dsh_doc: dict | None) -> None:
    try:
        scopes = parse_ssh_scopes(ssh_doc)
        if dsh_doc is not None and dsh_doc.get("hosts"):
            parse_dsh_transports(dsh_doc, scopes)
    except (TypeError, ValueError, KeyError) as error:
        raise HostAdminError("invalid_config", f"Host configuration is invalid: {error}") from None


def _configured_names(ssh_doc: dict) -> set[str]:
    names: set[str] = set()
    for entry in ssh_doc.get("hosts", []):
        if not isinstance(entry, dict) or not isinstance(entry.get("host"), str):
            continue
        names.add(entry["host"].casefold())
        for alias in entry.get("aliases", []):
            if isinstance(alias, str):
                names.add(alias.casefold())
    return names


def list_hosts(
    *, ssh_path: Path | None = None, dsh_path: Path | None = None,
    registry_path: Path | None = None,
) -> dict:
    ssh_path = ssh_path or config.SSH_CONFIG_PATH
    dsh_path = dsh_path or config.DSH_CONFIG_PATH
    registry_path = registry_path or config.REGISTRY_PATH
    ssh_doc = _read_doc(ssh_path)
    dsh_doc = _read_doc(dsh_path) if dsh_path.exists() else {"hosts": []}
    scopes, _ = read_document(registry_path)
    dsh_by_host = {
        str(entry.get("host", "")).casefold(): entry
        for entry in dsh_doc.get("hosts", []) if isinstance(entry, dict)
    }
    rows = []
    for entry in ssh_doc.get("hosts", []):
        if not isinstance(entry, dict) or not isinstance(entry.get("host"), str):
            continue
        host = entry["host"]
        dsh = dsh_by_host.get(host.casefold())
        rows.append({
            "host": host,
            "aliases": list(entry.get("aliases", [])),
            "project_roots": list(entry.get("project_roots", [])),
            "workspace_roots": list(entry.get("workspace_roots", [])),
            "remote_codex": entry.get("remote_codex"),
            "dsh": None if dsh is None else {
                "bridge": dsh.get("bridge_dir"),
                "profile": dsh.get("profile"),
            },
            "registered_workspaces": sorted(str(path) for path in scopes.get(host.casefold(), ())),
        })
    return {"hosts": rows}


def _default_control_root(host: str) -> Path:
    return Path.home() / ".dsh" / "gateway-ssh" / host.casefold()


def add_host(
    alias: str,
    *,
    aliases: tuple[str, ...] = (),
    project_roots: tuple[str, ...] = (),
    workspaces: tuple[str, ...] = (),
    remote_codex: str | None = None,
    dsh: bool | None = None,
    dsh_bridge: str | None = None,
    dsh_profile: str = "web",
    control_root: Path | None = None,
    ssh_path: Path | None = None,
    dsh_path: Path | None = None,
    check=inspect_ssh_host,
    probe=probe_remote,
    directory_check=remote_directory,
    file_check=remote_file,
    restart=None,
) -> dict:
    if not alias or not alias.isprintable() or " " in alias:
        raise HostAdminError("invalid_host", "Host must be one SSH alias or address.")
    host = alias.casefold()
    ssh_path = ssh_path or config.SSH_CONFIG_PATH
    dsh_path = dsh_path or config.DSH_CONFIG_PATH
    if host in _configured_names(_read_doc(ssh_path)):
        raise HostAdminError("host_exists", f"Host already configured: {alias}")
    candidate = check(alias)
    remote = probe(alias)
    home = remote.get("home")
    if not isinstance(home, str) or not PurePosixPath(home).is_absolute():
        raise HostAdminError("remote_probe_failed", "Remote home is invalid.")

    if project_roots:
        roots = tuple(PurePosixPath(value) for value in project_roots)
    else:
        roots = (PurePosixPath(home),)
    for path in roots:
        if not path.is_absolute() or not directory_check(alias, str(path)):
            raise HostAdminError("invalid_project", f"Remote project root does not exist: {path}")

    selected_workspaces = tuple(PurePosixPath(value) for value in workspaces) if workspaces else ()
    if not selected_workspaces and remote.get("workspace"):
        selected_workspaces = (PurePosixPath(remote["workspace"]),)
    for path in selected_workspaces:
        if not path.is_absolute() or not directory_check(alias, str(path)):
            raise HostAdminError("invalid_workspace", f"Remote workspace does not exist: {path}")

    bridge = dsh_bridge or str(PurePosixPath(home) / ".dsh" / "gateway-bridge")
    if not isinstance(bridge, str) or not PurePosixPath(bridge).is_absolute():
        raise HostAdminError("invalid_dsh", "DSH bridge directory must be absolute.")
    if not _PROFILE.fullmatch(dsh_profile):
        raise HostAdminError("invalid_dsh", "DSH profile is invalid.")
    if dsh_bridge:
        bridge_ready = (
            directory_check(alias, bridge)
            and file_check(alias, f"{bridge}/bridge-token")
            and file_check(alias, f"{bridge}/bridge-workspaces.json")
        )
    else:
        bridge_ready = bool(remote.get("bridge_exists") and remote.get("token") and remote.get("scope"))
    use_dsh = bridge_ready if dsh is None else dsh
    if use_dsh and not selected_workspaces:
        raise HostAdminError("invalid_workspace", "DSH registration needs at least one workspace.")
    if use_dsh and not bridge_ready and dsh is True:
        raise HostAdminError("dsh_bridge_incomplete", f"Remote DSH bridge is incomplete: {bridge}")

    control = (control_root or _default_control_root(host)).resolve()
    control.mkdir(mode=0o700, parents=True, exist_ok=True)
    control.chmod(0o700)
    control_path = control / "ssh"
    owner_socket = control / "ipc"
    if len(str(control_path).encode()) >= 104:
        raise HostAdminError("invalid_control", "SSH control socket path is too long.")

    entry = {
        "host": alias,
        "aliases": list(dict.fromkeys(aliases)),
        "project_roots": [str(path) for path in roots],
        "remote_codex": remote_codex or remote.get("codex") or "codex",
        "control_path": str(control_path),
        "owner_socket": str(owner_socket),
        "workspace_roots": [str(path) for path in selected_workspaces],
    }
    dsh_entry = None
    if use_dsh:
        dsh_entry = {
            "host": host,
            "profile": dsh_profile,
            "bridge_dir": bridge,
            "workspaces": [{"path": str(path)} for path in selected_workspaces],
        }

    with _lock(ssh_path):
        ssh_doc = _read_doc(ssh_path)
        dsh_doc = _read_doc(dsh_path) if dsh_path.exists() else {"hosts": []}
        existing = _configured_names(ssh_doc)
        if host in existing:
            raise HostAdminError("host_exists", f"Host already configured: {alias}")
        for name in entry["aliases"]:
            if str(name).casefold() in existing and str(name).casefold() != host:
                raise HostAdminError("alias_exists", f"Alias already configured: {name}")
        ssh_doc["hosts"].append(entry)
        if dsh_entry is not None:
            dsh_doc["hosts"].append(dsh_entry)
        _validate(ssh_doc, dsh_doc if dsh_entry is not None else None)
        _write_doc(ssh_path, ssh_doc)
        if dsh_entry is not None:
            _write_doc(dsh_path, dsh_doc)
    return {
        "host": alias,
        "candidate": {
            "alias": candidate.alias,
            "hostname": candidate.hostname,
            "user": candidate.user,
            "port": candidate.port,
        },
        "remote": remote,
        "dsh": dsh_entry,
        "remote_codex": entry["remote_codex"],
        "restart": restart() if restart is not None else None,
    }


def remove_host(
    alias: str, *, force: bool = False,
    ssh_path: Path | None = None, dsh_path: Path | None = None,
    registry_path: Path | None = None, restart=None,
) -> dict:
    host = alias.casefold()
    ssh_path = ssh_path or config.SSH_CONFIG_PATH
    dsh_path = dsh_path or config.DSH_CONFIG_PATH
    registry_path = registry_path or config.REGISTRY_PATH
    with _lock(ssh_path):
        ssh_doc = _read_doc(ssh_path)
        def matches(entry: object) -> bool:
            if not isinstance(entry, dict):
                return False
            names = {str(entry.get("host", "")).casefold()}
            names.update(str(name).casefold() for name in entry.get("aliases", []))
            return host in names

        match = next((entry for entry in ssh_doc.get("hosts", []) if matches(entry)), None)
        if match is None:
            raise HostAdminError("host_not_found", f"Host is not configured: {alias}")
        canonical = str(match["host"]).casefold()
        registered = read_document(registry_path)[0].get(canonical, ())
        if registered and not force:
            raise HostAdminError(
                "workspaces_registered",
                f"{alias} still has {len(registered)} registered workspace(s); use --force.",
            )
        ssh_doc["hosts"] = [
            entry for entry in ssh_doc["hosts"]
            if not (isinstance(entry, dict) and str(entry.get("host", "")).casefold() == canonical)
        ]
        dsh_doc = _read_doc(dsh_path) if dsh_path.exists() else {"hosts": []}
        dsh_doc["hosts"] = [
            entry for entry in dsh_doc.get("hosts", [])
            if not (isinstance(entry, dict) and str(entry.get("host", "")).casefold() == canonical)
        ]
        _validate(ssh_doc, dsh_doc if dsh_doc.get("hosts") else None)
        _write_doc(ssh_path, ssh_doc)
        if dsh_doc.get("hosts") or dsh_path.exists():
            _write_doc(dsh_path, dsh_doc)
        if registered and force:
            with registry_lock(registry_path):
                scopes, homes = read_document(registry_path)
                scopes.pop(canonical, None)
                homes.pop(canonical, None)
                write_registry(scopes, homes, registry_path)
    return {"host": match["host"], "removed_workspaces": len(registered), "restart": restart() if restart is not None else None}
