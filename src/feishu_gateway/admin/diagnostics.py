"""Read Gateway registration and check the scoped session catalogs."""

import asyncio
import os
import shlex
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path, PurePosixPath

from feishu_gateway.executors.codex_app_server import PreviewError
from feishu_gateway.executors.dsh import DSHReadTransport, DSHWorkspace
from feishu_gateway.catalog.hosts import SSHScope, discover_host_catalog
from feishu_gateway.executors.codex_remote import SSHReadTransport


Registry = Mapping[str, tuple[PurePosixPath, ...]]


def registered_workspaces(ssh_scopes: Sequence[SSHScope], registry: Registry, host: str | None = None) -> dict:
    known = {"local", *(scope.transport.host for scope in ssh_scopes)}
    if host is not None and host not in known:
        return {"status": "host_not_configured", "host": host, "hosts": []}
    selected = sorted(known) if host is None else [host]
    hosts = [
        {"host": name, "workspaces": sorted(str(path) for path in registry.get(name, ()))}
        for name in selected if host is not None or registry.get(name)
    ]
    return {"status": "available" if any(row["workspaces"] for row in hosts) else "empty", "host": host, "hosts": hosts}


async def _run_probe(command: Sequence[str]) -> int | None:
    process = None
    try:
        async with asyncio.timeout(8):
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env={key: os.environ[key] for key in ("HOME", "PATH", "USER", "LOGNAME") if key in os.environ},
            )
            return await process.wait()
    except (OSError, TimeoutError):
        return None
    finally:
        if process is not None and process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()


async def _check_ssh(ssh: SSHReadTransport) -> dict:
    if ssh.control_path is None:
        return {"status": "not_configured", "reason": "ssh_control_not_configured"}
    try:
        ssh._check_control_path()
    except PreviewError:
        return {"status": "unavailable", "reason": "ssh_control_unavailable"}
    status = await _run_probe(ssh._ssh_command("exec /bin/true"))
    if status == 0:
        return {"status": "connected"}
    return {"status": "unavailable", "reason": "ssh_connection_unavailable"}


# A generic bridge error can cover several failures. Check the private socket
# only in that case, without reading the bridge credential or sending data.
_DSH_SOCKET_CHECK = """
import os
import socket
import stat
import sys
from pathlib import Path

try:
    path = Path(sys.argv[1])
    parent = path.parent.lstat()
    info = path.lstat()
    if (not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode) != 0o700
            or not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600):
        sys.exit(13)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(str(path))
except FileNotFoundError:
    sys.exit(11)
except ConnectionRefusedError:
    sys.exit(12)
except PermissionError:
    sys.exit(13)
except OSError:
    sys.exit(14)
"""


async def _dsh_socket_reason(transport: DSHReadTransport) -> str:
    arguments = ("-I", "-c", _DSH_SOCKET_CHECK, str(transport.remote_dir / "bridge.sock"))
    ssh = transport.ssh
    if ssh is None:
        command = (sys.executable, *arguments)
    else:
        try:
            ssh._check_control_path()
        except PreviewError:
            return "dsh_bridge_unavailable"
        command = ssh._ssh_command("exec " + shlex.join(("python3", *arguments)))
    return {
        11: "dsh_socket_missing",
        12: "dsh_socket_refused",
        13: "dsh_socket_permissions",
    }.get(await _run_probe(command), "dsh_bridge_unavailable")


def _catalog(status: str, session_count: int | None = None, reason: str | None = None) -> dict:
    result = {"status": status, "session_count": session_count}
    if reason is not None:
        result["reason"] = reason
    return result


def _count(rows, host: str, roots: tuple[PurePosixPath, ...]) -> int:
    return sum(row.target.host == host and row.cwd in roots for row in rows)


async def _check_codex(
    codex_command: str | None, host: str, scope: SSHScope | None, roots: tuple[PurePosixPath, ...],
) -> dict:
    if codex_command is None and scope is None:
        return _catalog("not_configured")
    ssh_scopes = () if scope is None else (
        replace(scope, project_roots=roots, catalog_only_roots=(), workspace_roots=(), writable_sessions={}),
    )
    results = await discover_host_catalog(
        codex_command=codex_command or "codex", ssh_scopes=ssh_scopes,
        hosts=(host,), local_roots=tuple(Path(root) for root in roots) if host == "local" else (),
    )
    result = results[host]
    if result.error:
        return _catalog("unavailable", reason=result.error)
    count = _count(result.candidates, host, roots)
    return _catalog("available" if count else "empty", count)


async def _check_dsh(transport: DSHReadTransport | None, host: str, roots: tuple[PurePosixPath, ...]) -> dict:
    if transport is None:
        return _catalog("not_configured")
    try:
        scoped = replace(transport, project_root=None, session_id=None, workspaces=tuple(DSHWorkspace(p) for p in roots))
        rows = await scoped.discover()
    except ValueError:
        return _catalog("unavailable", reason="dsh_scope_invalid")
    except PreviewError as error:
        reason = await _dsh_socket_reason(transport) if error.code == "dsh_bridge_unavailable" else error.code
        return _catalog("unavailable", reason=reason)
    count = _count(rows, host, roots)
    return _catalog("available" if count else "empty", count)


async def diagnose(
    *, ssh_scopes: Sequence[SSHScope], dsh_transports: Sequence[DSHReadTransport], registry: Registry,
    codex_command: str | None, gateway_status: Mapping[str, str], host: str | None = None,
    workspace: PurePosixPath | None = None, router_status: str | None = None,
) -> dict:
    """Check gateway health and the selected exact registered scope, without writing it."""
    if workspace is not None and host is None:
        raise ValueError("A workspace diagnosis requires a host.")
    roots = tuple(registry.get(host, ())) if host is not None else ()
    scope = next((entry for entry in ssh_scopes if entry.transport.host == host), None)
    if host is None:
        registration = "not_requested"
    elif host != "local" and scope is None:
        registration = "host_not_configured"
        roots = ()
    elif workspace is not None and workspace not in roots:
        registration = "workspace_not_registered"
    else:
        registration = "registered" if roots else "no_registered_workspaces"

    result = {
        "gateway": {key: gateway_status.get(key, "unknown") for key in ("process", "feishu")},
        "host": host,
        "workspace": str(workspace) if workspace is not None else None,
        "registration": {"status": registration, "workspaces": sorted(str(path) for path in roots)},
        "ssh": {"status": "not_checked"},
        "codex": _catalog("not_checked"),
        "dsh": _catalog("not_checked"),
    }
    if router_status is not None:
        result["router"] = {"status": router_status}
    if host is None:
        return result
    if host == "local":
        result["ssh"] = {"status": "not_applicable"}
    elif scope is None:
        result["ssh"] = {"status": "not_configured"}
    else:
        result["ssh"] = await _check_ssh(scope.transport)
    if registration != "registered":
        return result

    transport = next((entry for entry in dsh_transports if entry.host == host), None)
    if result["ssh"]["status"] in {"not_configured", "unavailable"}:
        result["codex"] = _catalog("unavailable", reason="ssh_unavailable")
        result["dsh"] = _catalog("not_configured") if transport is None else _catalog("unavailable", reason="ssh_unavailable")
        return result
    selected_roots = (workspace,) if workspace is not None else roots
    result["codex"], result["dsh"] = await asyncio.gather(
        _check_codex(codex_command, host, scope, selected_roots),
        _check_dsh(transport, host, selected_roots),
    )
    return result
