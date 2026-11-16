"""Configure trusted hosts and query their scoped Codex catalogs independently."""

import asyncio
from collections.abc import Awaitable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from unicodedata import normalize
from uuid import UUID

from feishu_gateway.executors.codex_app_server import PreviewError
from feishu_gateway.catalog.sessions import discover_sessions
from feishu_gateway.catalog.types import SessionCandidate
from feishu_gateway.executors.codex_remote import (
    SSHReadTransport, canonical_remote_cwd, discover_remote_sessions,
    discover_remote_workspace_sessions,
)


_REQUIRED = {"host", "remote_codex", "control_path", "owner_socket"}
_ROOT_KEYS = {"project_root", "project_roots"}
_OPTIONAL = {"aliases", "catalog_only_roots", "writable_sessions", "workspace_roots"}
_RESERVED_ALIASES = {"local", "本机", "本地", "远端", "远程"}


@dataclass(frozen=True, slots=True)
class SSHScope:
    transport: SSHReadTransport
    project_roots: tuple[PurePosixPath, ...]
    owner_socket: Path | None
    aliases: tuple[str, ...] = ()
    catalog_only_roots: tuple[PurePosixPath, ...] = ()
    writable_sessions: Mapping[str, PurePosixPath] = field(
        default_factory=lambda: MappingProxyType({})
    )
    workspace_roots: tuple[PurePosixPath, ...] = ()


@dataclass(frozen=True, slots=True)
class HostCatalogResult:
    candidates: tuple[SessionCandidate, ...]
    error: str | None = None


def _socket_path(value: object) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isprintable() or "%" in value:
        raise ValueError("SSH 套接字路径无效。")
    path = Path(value)
    if not path.is_absolute() or not path.name or ".." in path.parts or str(path) != value:
        raise ValueError("SSH 套接字须使用绝对路径。")
    return path


def _project_root(value: object) -> PurePosixPath:
    if not isinstance(value, str) or not value.isprintable():
        raise ValueError("远端项目目录无效。")
    root = PurePosixPath(value)
    if not root.is_absolute() or not root.name or ".." in root.parts or str(root) != value:
        raise ValueError("远端项目须使用绝对路径。")
    return root


def _project_roots(values: object) -> tuple[PurePosixPath, ...]:
    if not isinstance(values, list):
        raise ValueError("远端授权目录须使用列表。")
    return tuple(_project_root(value) for value in values)


def _writable_sessions(value: object, roots: tuple[PurePosixPath, ...]) -> Mapping[str, PurePosixPath]:
    if not isinstance(value, dict):
        raise ValueError("单独授权的会话须使用会话 ID 与目录的映射。")
    result = {}
    for session_id, value_root in value.items():
        if not isinstance(session_id, str) or str(UUID(session_id)) != session_id:
            raise ValueError("单独授权的会话须使用完整 UUID。")
        root = _project_root(value_root)
        if root not in roots:
            raise ValueError("单独授权的会话须位于仅供查询的目录。")
        result[session_id] = root
    return MappingProxyType(result)


def _parse_host(entry: object, *, legacy: bool) -> SSHScope:
    if not isinstance(entry, dict):
        raise ValueError("每台远端主机须使用独立配置对象。")
    root_key = entry.keys() & _ROOT_KEYS
    if (
        not _REQUIRED <= entry.keys()
        or not entry.keys() <= _REQUIRED | _ROOT_KEYS | _OPTIONAL
        or len(root_key) > 1
        or (legacy and entry.keys() != _REQUIRED | {"project_root"})
    ):
        raise ValueError("远端主机配置字段无效。")
    ssh_alias = entry["host"]
    if not isinstance(ssh_alias, str) or ssh_alias.casefold() in _RESERVED_ALIASES:
        raise ValueError("远端主机别名无效。")

    roots = _project_roots(
        [entry["project_root"]] if "project_root" in entry else entry.get("project_roots", [])
    )
    catalog_only = _project_roots(entry.get("catalog_only_roots", []))
    workspaces = _project_roots(entry.get("workspace_roots", []))
    discovery_roots = roots + catalog_only
    if not discovery_roots and not workspaces:
        raise ValueError("至少指定一个远端授权项目目录。")
    if len(set(discovery_roots)) != len(discovery_roots):
        raise ValueError("同一主机的项目目录重复。")
    if any(canonical_remote_cwd(str(root)) is None for root in workspaces) or len(set(workspaces)) != len(workspaces):
        raise ValueError("远端工作区路径必须规范且不能重复。")
    writable_sessions = _writable_sessions(entry.get("writable_sessions", {}), catalog_only)

    control = _socket_path(entry["control_path"])
    owner = _socket_path(entry["owner_socket"])
    if legacy and (control is None or owner is None):
        raise ValueError("旧格式须指定 SSH 控制和写入方套接字。")
    if control is not None and owner is not None and (owner == control or owner.parent != control.parent):
        raise ValueError("SSH 控制和写入方须在同一目录下使用不同套接字。")

    raw_aliases = entry.get("aliases", [])
    if not isinstance(raw_aliases, list):
        raise ValueError("远端主机的易读名称须使用列表。")
    aliases = []
    for alias in raw_aliases:
        if not isinstance(alias, str) or not alias.isprintable():
            raise ValueError("远端主机的易读名称无效。")
        alias = normalize("NFC", alias.strip().casefold())
        if not alias or any(char in alias for char in "/／:：「」"):
            raise ValueError("远端主机的易读名称无效。")
        aliases.append(alias)

    host = ssh_alias.casefold()
    return SSHScope(
        SSHReadTransport(host, entry["remote_codex"], control, ssh_alias=ssh_alias),
        roots, owner, tuple(aliases), catalog_only, writable_sessions, workspaces,
    )


def parse_ssh_scopes(data: object) -> tuple[SSHScope, ...]:
    """Accept the original single host or a hosts array in the private config."""
    if data is None:
        return ()
    if not isinstance(data, dict):
        raise ValueError("远端主机配置须使用对象。")
    if "hosts" in data:
        if data.keys() != {"hosts"} or not isinstance(data["hosts"], list):
            raise ValueError("远端主机配置须包含 hosts 列表。")
        scopes = tuple(_parse_host(entry, legacy=False) for entry in data["hosts"])
    else:
        scopes = (_parse_host(data, legacy=True),)

    names = set(_RESERVED_ALIASES)
    socket_paths = set()
    for scope in scopes:
        for name in (scope.transport.host, *scope.aliases):
            if name in names:
                raise ValueError("远端主机名称或别名重复。")
            names.add(name)
        for path in (scope.transport.control_path, scope.owner_socket):
            if path is not None:
                if path in socket_paths:
                    raise ValueError("不同远端主机须使用独立套接字。")
                socket_paths.add(path)
    return scopes


async def _capture(query: Awaitable[tuple[SessionCandidate, ...]]) -> HostCatalogResult:
    try:
        return HostCatalogResult(await query)
    except PreviewError as error:
        return HostCatalogResult((), error.code)
    except (OSError, TimeoutError):
        return HostCatalogResult((), "catalog_unavailable")


async def _discover_remote(scope: SSHScope) -> HostCatalogResult:
    if scope.workspace_roots:
        return await _capture(discover_remote_workspace_sessions(
            scope.transport, workspace_roots=scope.workspace_roots,
            exact_roots=scope.project_roots + scope.catalog_only_roots,
        ))
    slots = asyncio.Semaphore(4)

    async def query(root: PurePosixPath) -> HostCatalogResult:
        async with slots:
            return await _capture(discover_remote_sessions(scope.transport, project_root=root))

    results = await asyncio.gather(*(query(root) for root in scope.project_roots + scope.catalog_only_roots))
    candidates = {}
    duplicated = set()
    error = None
    for result in results:
        error = error or result.error
        for candidate in result.candidates:
            target = candidate.target
            if target in candidates:
                duplicated.add(target)
            candidates[target] = candidate
    if duplicated:
        error = error or "catalog_changed"
        for target in duplicated:
            del candidates[target]
    return HostCatalogResult(tuple(candidates.values()), error)


async def discover_host_catalog(
    *,
    codex_command: str,
    project_root: Path | None = None,
    ssh_scopes: Sequence[SSHScope],
    hosts: Collection[str] | None = None,
    local_roots: Sequence[Path] | None = None,
) -> dict[str, HostCatalogResult]:
    """Query registered hosts concurrently and preserve each host's result."""
    remote = {scope.transport.host: scope for scope in ssh_scopes}
    names = ("local", *remote)
    requested = set(names) if hosts is None else set(hosts)
    if not requested <= set(names):
        raise ValueError("只能查询已配置的主机。")
    async def query(name: str) -> HostCatalogResult:
        if name != "local":
            return await _discover_remote(remote[name])
        if local_roots is not None:
            return await _discover_local(codex_command, local_roots)
        if project_root is not None:
            return await _capture(discover_sessions(codex_command=codex_command, project_root=project_root))
        return HostCatalogResult(())

    selected = [name for name in names if name in requested]
    queries = [query(name) for name in selected]
    return dict(zip(selected, await asyncio.gather(*queries), strict=True))


async def _discover_local(codex_command: str, roots: Sequence[Path]) -> HostCatalogResult:
    slots = asyncio.Semaphore(4)

    async def query(root: Path) -> HostCatalogResult:
        async with slots:
            try:
                if root.resolve(strict=True) != root or not root.is_dir():
                    return HostCatalogResult((), "catalog_scope")
            except (OSError, RuntimeError, ValueError):
                return HostCatalogResult((), "catalog_scope")
            result = await _capture(discover_sessions(codex_command=codex_command, project_root=root))
            if any(candidate.cwd != root for candidate in result.candidates):
                return HostCatalogResult((), "catalog_scope")
            return result

    candidates = {}
    mismatched = set()
    reason = None
    for result in await asyncio.gather(*(query(root) for root in roots)):
        reason = reason or result.error
        for candidate in result.candidates:
            existing = candidates.setdefault(candidate.target, candidate)
            if existing.cwd != candidate.cwd:
                mismatched.add(candidate.target)
    if mismatched:
        for target in mismatched:
            del candidates[target]
        reason = reason or "catalog_changed"
    return HostCatalogResult(tuple(candidates.values()), reason)
