"""Discover sessions inside the exact registered workspace scope."""

from __future__ import annotations

import asyncio
import weakref
from logging import getLogger
from dataclasses import replace
from functools import partial
from pathlib import Path, PurePosixPath

from feishu_gateway import config as gateway_config
from feishu_gateway.catalog.hosts import SSHScope, discover_host_catalog
from feishu_gateway.catalog.types import SessionCandidate
from feishu_gateway.catalog.types import CatalogSnapshot, RoutedInstruction
from feishu_gateway.executors.codex_app_server import PreviewError
from feishu_gateway.executors.dsh import DSHReadTransport, DSHWorkspace
from feishu_gateway.executors.backends import (
    CodexBackend, codex_backend_for, codex_session_status, dsh_session_status,
)
from feishu_gateway.workspaces.registry import read_registry


LOGGER = getLogger("feishu-gateway")


async def _discover_sessions(
    *, codex_command: str, project_root: Path | None = None, ssh_scopes: tuple[SSHScope, ...],
    dsh_transports: tuple[DSHReadTransport, ...], log_failures: bool = True,
    local_roots: tuple[Path, ...] | None = None,
    hosts: tuple[str, ...] | None = None,
) -> CatalogSnapshot:
    async def discover_dsh(transport):
        try:
            return transport.host, await transport.discover(), None
        except PreviewError as error:
            return transport.host, (), error.code

    results, *dsh_results = await asyncio.gather(
        discover_host_catalog(
            codex_command=codex_command, project_root=project_root, ssh_scopes=ssh_scopes,
            local_roots=local_roots, hosts=hosts,
        ),
        *(discover_dsh(transport) for transport in dsh_transports),
    )
    candidates = []
    unavailable = set()
    for host, result in results.items():
        candidates.extend(result.candidates)
        if result.error:
            if log_failures:
                LOGGER.warning("%s 的 Codex 会话目录暂不可查（%s）。", host, result.error)
            unavailable.add(("codex", host))
    for host, sessions, error in dsh_results:
        candidates.extend(sessions)
        if error:
            if log_failures:
                LOGGER.warning("%s 的 DSH 会话目录暂不可查（%s）。", host, error)
            unavailable.add(("dsh", host))
    return CatalogSnapshot(tuple(candidates), unavailable_backends=frozenset(unavailable))


def _workspace_scopes(
    registry: dict[str, tuple[PurePosixPath, ...]], ssh_scopes: tuple[SSHScope, ...],
    dsh_transports: tuple[DSHReadTransport, ...],
) -> tuple[tuple[Path, ...], tuple[SSHScope, ...], tuple[DSHReadTransport, ...]]:
    local = tuple(Path(path) for path in registry.get("local", ()))
    ssh = tuple(
        replace(scope, project_roots=registry.get(scope.transport.host, ()),
                catalog_only_roots=(), writable_sessions={}, workspace_roots=())
        for scope in ssh_scopes
    )
    configured = {transport.host: transport for transport in dsh_transports}
    dsh = []
    for scope in ssh_scopes:
        host = scope.transport.host
        workspaces = registry.get(host, ())
        if not workspaces:
            continue
        transport = configured.get(host)
        if transport is None:
            # The bridge is installed at the conventional remote path. Do not
            # require a hand-written bridge_dir entry when the SSH host and
            # workspace are already authorized.
            home = scope.project_roots[0] if scope.project_roots else None
            if home is None:
                continue
            transport = DSHReadTransport(
                ssh=scope.transport,
                remote_dir=home / ".dsh" / "gateway-bridge",
                profile="web",
                host=host,
                workspaces=tuple(DSHWorkspace(path) for path in workspaces),
            )
        dsh.append(replace(
            transport, project_root=None, session_id=None,
            workspaces=tuple(DSHWorkspace(path) for path in workspaces),
        ))
    return local, ssh, tuple(dsh)


def _registered_snapshot(
    snapshot: CatalogSnapshot, registry: dict[str, tuple[PurePosixPath, ...]],
) -> CatalogSnapshot:
    return replace(snapshot, candidates=tuple(
        candidate for candidate in snapshot.candidates
        if PurePosixPath(candidate.cwd) in registry.get(candidate.target.host, ())
    ))


async def _scan_sessions(
    host: str | None = None, workspace: PurePosixPath | None = None,
    *, registry: dict[str, tuple[PurePosixPath, ...]] | None = None, report=print,
    rows_out: list[dict] | None = None,
) -> int:
    settings = gateway_config.read_config(gateway_config.CONFIG_PATH)
    codex_command = settings.codex_command if settings is not None else "codex"
    ssh_scopes = gateway_config.read_ssh_scopes(gateway_config.SSH_CONFIG_PATH)
    dsh_transports = gateway_config.read_dsh_transports(gateway_config.DSH_CONFIG_PATH, ssh_scopes)
    if registry is None:
        registry = read_registry()
    registered = {name: paths for name, paths in registry.items() if paths and (host is None or name == host)}
    if workspace is not None:
        registered = {name: (workspace,) for name, paths in registered.items() if workspace in paths}
    if not registered:
        report("No registered workspaces.")
        return 0
    known_hosts = {"local", *(scope.transport.host for scope in ssh_scopes)}
    if not registered.keys() <= known_hosts:
        report("An authorized host is no longer configured.")
        return 2
    local_roots, ssh_scopes, dsh_transports = _workspace_scopes(registered, ssh_scopes, dsh_transports)
    snapshot = await _discover_sessions(
        codex_command=codex_command, local_roots=local_roots, hosts=tuple(registered),
        ssh_scopes=ssh_scopes, dsh_transports=dsh_transports, log_failures=False,
    )
    snapshot = _registered_snapshot(snapshot, registered)
    exact, workspaces, _ = _codex_backends(codex_command, local_roots, ssh_scopes, verify_exact=True)
    dsh_by_host = {transport.host: transport for transport in dsh_transports}
    slots = asyncio.Semaphore(8)

    async def check(candidate: SessionCandidate) -> str:
        target = candidate.target
        routed = RoutedInstruction(target, "", intent="query", cwd=candidate.cwd)
        async with slots:
            try:
                async with asyncio.timeout(12):
                    if target.runtime == "dsh":
                        return await dsh_session_status(routed, dsh_by_host.get(target.host))
                    backend = codex_backend_for(
                        str(candidate.cwd), exact.get(target.host, ()), workspaces.get(target.host, ()),
                    )
                    if backend is not None:
                        state = await codex_session_status(routed, backend)
                        if state == "unknown":
                            await asyncio.sleep(0.25)
                            state = await codex_session_status(routed, backend)
                        return state
            except (PreviewError, TimeoutError, OSError):
                pass
        return "unknown"

    report(f"Found {len(snapshot.candidates)} sessions. Checking status...", flush=True)
    states = dict(zip(snapshot.candidates, await asyncio.gather(
        *(check(candidate) for candidate in snapshot.candidates),
    ), strict=True))
    labels = {"codex": "Codex", "dsh": "DSH"}
    status_labels = {
        "idle": "Idle",
        "busy": "Busy",
        "read_only": "Read-only",
        "owner_not_configured": "Execution endpoint not configured",
        "owner_unavailable": "Owner unavailable",
        "owner_rejected": "External input disabled",
        "owner_state_unknown": "Owner online; state unknown",
        "remote_unavailable": "Workspace check unavailable",
        "session_unavailable": "Session unavailable",
        "execution_policy_unavailable": "Execution permissions unknown",
        "scope_mismatch": "Outside authorized scope",
        "unattached": "Session not loaded",
        "resumable": "Session can be resumed on delivery",
        "unknown": "Status unknown",
    }
    backends = {
        *(("codex", name) for name in registered),
        *(("dsh", transport.host) for transport in dsh_transports),
    }
    by_source = {source: [] for source in backends}
    for candidate in snapshot.candidates:
        target = candidate.target
        by_source[(target.runtime, target.host)].append(candidate)

    for (runtime, host), candidates in sorted(by_source.items(), key=lambda item: (item[0][1], item[0][0])):
        unavailable = (runtime, host) in snapshot.unavailable_backends or host in snapshot.unavailable_hosts
        if unavailable:
            status = f"{len(candidates)} (partial)" if candidates else "Catalog unavailable"
        else:
            status = str(len(candidates)) if candidates else "No sessions"
        report(f"{host} / {labels[runtime]}: {status}")
        for candidate in sorted(candidates, key=lambda row: ((row.title or "").casefold(), row.target.session_id)):
            title = "".join(char if char.isprintable() else " " for char in candidate.title or "").strip()
            state = states[candidate]
            if rows_out is not None:
                rows_out.append({
                    "host": candidate.target.host,
                    "runtime": candidate.target.runtime,
                    "session_id": candidate.target.session_id,
                    "title": title or None,
                    "cwd": str(candidate.cwd),
                    "state": state,
                    "state_label": status_labels.get(state, "Status unknown"),
                })
            report(f"  {title or 'Untitled'} | {status_labels.get(state, 'Status unknown')} | {candidate.target.session_id}")
    return 2 if snapshot.unavailable_backends or snapshot.unavailable_hosts else 0


def _codex_backends(
    codex_command: str, local_roots: Path | tuple[Path, ...], ssh_scopes: tuple[SSHScope, ...],
    *, verify_exact: bool = False,
):
    if isinstance(local_roots, Path):
        local_roots = (local_roots,)
    backends = {"local": tuple(CodexBackend(root, codex_command) for root in local_roots)}
    workspace_backends = {}
    host_aliases = {"local": "local", "本机": "local", "本地": "local"}
    for ssh_scope in ssh_scopes:
        host = ssh_scope.transport.host
        host_aliases.update((alias, host) for alias in (host, *ssh_scope.aliases))
        remote_backends = []
        for root in (*ssh_scope.project_roots, *ssh_scope.catalog_only_roots):
            remote_root = Path(str(root))
            allowed = None if root in ssh_scope.project_roots else frozenset(
                thread_id for thread_id, allowed_root in ssh_scope.writable_sessions.items()
                if allowed_root == root
            )
            remote_backends.append(CodexBackend(
                remote_root, None, ssh_scope.transport.connect,
                ssh_scope.owner_socket, allowed,
                check_cwd=(partial(ssh_scope.transport.check_workspace_cwd, roots=(root,)) if verify_exact else None),
                ensure_owner=(
                    partial(ssh_scope.transport.ensure_owner_forward, ssh_scope.owner_socket, force=False)
                    if ssh_scope.owner_socket is not None else None
                ),
                repair_owner=(
                    partial(ssh_scope.transport.ensure_owner_forward, ssh_scope.owner_socket, force=True)
                    if ssh_scope.owner_socket is not None else None
                ),
                transport=ssh_scope.transport,
            ))
        backends[host] = tuple(remote_backends)
        workspace_backends[host] = tuple(
            CodexBackend(
                Path(root), None, ssh_scope.transport.connect, ssh_scope.owner_socket,
                check_cwd=partial(ssh_scope.transport.check_workspace_cwd, roots=(root,)),
                ensure_owner=(
                    partial(ssh_scope.transport.ensure_owner_forward, ssh_scope.owner_socket, force=False)
                    if ssh_scope.owner_socket is not None else None
                ),
                repair_owner=(
                    partial(ssh_scope.transport.ensure_owner_forward, ssh_scope.owner_socket, force=True)
                    if ssh_scope.owner_socket is not None else None
                ),
                transport=ssh_scope.transport,
            )
            for root in ssh_scope.workspace_roots
        )
    return backends, workspace_backends, host_aliases

discover_sessions = _discover_sessions
workspace_scopes = _workspace_scopes
registered_snapshot = _registered_snapshot
scan_sessions = _scan_sessions


_SCAN_LOCKS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _scan_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _SCAN_LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _SCAN_LOCKS[loop] = lock
    return lock


async def scan_session_rows(
    host: str | None = None, workspace: PurePosixPath | None = None,
    *, registry: dict[str, tuple[PurePosixPath, ...]] | None = None,
) -> tuple[list[dict], int]:
    # Codex app-server inventory is a single local process owner per scan;
    # overlapping scans can get server_closed from a competing child.
    async with _scan_lock():
        rows: list[dict] = []
        status = await _scan_sessions(
            host, workspace, registry=registry, report=lambda *args, **kwargs: None, rows_out=rows,
        )
        return rows, status
codex_backends = _codex_backends
