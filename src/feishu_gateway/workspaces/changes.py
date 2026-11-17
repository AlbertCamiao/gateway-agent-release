"""Plan and apply exact workspace registration changes for CLI and Feishu."""

import asyncio
from dataclasses import dataclass
from pathlib import PurePosixPath

from feishu_gateway.executors.codex_app_server import PreviewError
from feishu_gateway.workspaces.target import (
    check_workspace, host_for, ssh_destination, target_home, update_dsh_roots, verify_ssh_endpoint, workspace_path,
)
from feishu_gateway.workspaces.registry import WorkspaceRegistryError, absolute_workspace, read_document, registry_lock, write_registry


class WorkspaceChangeError(WorkspaceRegistryError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class _WorkspaceTarget:
    ssh: object | None
    bridge: object | None
    destination: tuple[str, str, int] | None


@dataclass(frozen=True, slots=True)
class WorkspaceChangeResult:
    status: int
    host: str
    workspace: PurePosixPath | None
    changed: bool = False
    already_registered: bool = False
    bridge_sync_pending: bool = False


def _target(host, ssh_scopes, dsh_transports):
    ssh = next((scope.transport for scope in ssh_scopes if scope.transport.host == host), None)
    if host != 'local' and ssh is None:
        raise WorkspaceChangeError('unknown_host', 'The host is not configured in Gateway.')
    bridge = next((transport for transport in dsh_transports if transport.host == host), None)
    destination = ssh_destination(ssh) if ssh is not None else None
    return _WorkspaceTarget(ssh, bridge, destination)


def _connection(target):
    if target.ssh is None:
        return None
    ssh = target.ssh
    if ssh.control_path is None:
        raise WorkspaceChangeError('ssh_unavailable', 'An authenticated SSH control connection is required.')
    try:
        ssh._check_control_path()
        before = ssh.control_path.lstat()
        endpoint = verify_ssh_endpoint(ssh, target.destination)
        after = ssh.control_path.lstat()
    except (OSError, PreviewError, WorkspaceRegistryError) as error:
        raise WorkspaceChangeError('ssh_unavailable', 'Could not verify the authenticated SSH connection.') from error
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise WorkspaceChangeError('ssh_changed', 'The SSH control connection changed. Preview the change again.')
    return before.st_dev, before.st_ino, endpoint


def _checked_directory(target, workspace):
    try:
        return check_workspace(target.ssh, workspace)
    except WorkspaceRegistryError as error:
        raise WorkspaceChangeError('invalid_workspace',
                                   'Workspace must be an existing canonical directory on the target host.') from error
    except PreviewError as error:
        raise WorkspaceChangeError('ssh_unavailable', 'Could not connect to the target host.') from error


def _apply_locked(action, host, path, target, scopes, homes, *, registry_path, scan, recheck=None,
                  check_bridge=None, bridge_sync_pending=False):
    roots = scopes.get(host, ())
    registered = path in roots
    bridge = target.bridge
    if action == 'remove':
        if not registered:
            return WorkspaceChangeResult(1, host, path)
        remaining = tuple(root for root in roots if root != path)
        write_registry({**scopes, host: remaining}, homes, registry_path)
        if bridge is not None:
            try:
                if check_bridge is not None:
                    check_bridge()
                update_dsh_roots(bridge, remaining)
            except (ValueError, PreviewError, OSError):
                bridge_sync_pending = True
        return WorkspaceChangeResult(0, host, path, changed=True, already_registered=True,
                                     bridge_sync_pending=bridge_sync_pending)

    if recheck is None:
        _checked_directory(target, path)
    else:
        recheck()
    home = homes.get(host) or target_home(target.ssh)
    expanded = roots if registered else (*roots, path)
    next_scopes = {**scopes, host: expanded}
    changed = False
    scope_update_attempted = False

    def restore_bridge():
        if not scope_update_attempted or registered:
            return False
        try:
            if check_bridge is not None:
                check_bridge()
            update_dsh_roots(bridge, roots)
        except (ValueError, PreviewError, OSError):
            return True
        return False

    try:
        if check_bridge is not None and bridge is not None:
            check_bridge()
        scope_update_attempted = bridge is not None
        update_dsh_roots(bridge, expanded)
        status = asyncio.run(scan(host, path, registry=next_scopes))
        # A missing optional catalog (for example Codex on a DSH-only host)
        # must not prevent the user from authorizing a verified workspace.
        if status in {0, 2} and not registered:
            if recheck is not None:
                recheck()
            write_registry(next_scopes, {**homes, host: home}, registry_path)
            changed = True
    except BaseException as error:
        if restore_bridge() and isinstance(error, Exception):
            raise WorkspaceChangeError('bridge_sync_pending',
                                       'Registration was not applied. DSH scope still needs synchronization.') from error
        raise
    pending = not changed and restore_bridge()
    return WorkspaceChangeResult(status, host, path, changed=changed, already_registered=registered,
                                 bridge_sync_pending=pending)


def change_workspace_from_cli(action, destination, value, *, load_ssh, load_dsh, scan, registry_path=None):
    """Apply the CLI's immediate scan/remove while retaining its relative path syntax."""
    with registry_lock(registry_path):
        ssh_scopes = load_ssh()
        host, ssh = host_for(destination, ssh_scopes)
        scopes, homes = read_document(registry_path)
        if action == 'remove' and not scopes.get(host):
            return WorkspaceChangeResult(1, host, None)
        path = workspace_path(value, homes.get(host) or target_home(ssh))
        sync_pending = False
        try:
            bridge = next((transport for transport in load_dsh(ssh_scopes) if transport.host == host), None)
        except (ValueError, OSError):
            if action != 'remove':
                raise
            bridge = None
            sync_pending = True
        target = _WorkspaceTarget(ssh, bridge, ssh_destination(ssh) if ssh is not None and action == 'scan' else None)
        recheck = None
        connection = None
        if action == 'scan' and ssh is not None and (ssh.control_path is not None or bridge is not None):
            connection = _connection(target)
            directory = _checked_directory(target, path)

            def recheck():
                if _connection(target) != connection:
                    raise WorkspaceChangeError('ssh_changed', 'The SSH connection changed. Scan again.')
                if _checked_directory(target, path) != directory:
                    raise WorkspaceChangeError('workspace_changed', 'The directory changed. Scan again.')

        def check_bridge():
            if ssh is not None:
                if action == 'scan':
                    if _connection(target) != connection:
                        raise WorkspaceChangeError('ssh_changed', 'The SSH connection changed. Scan again.')
                else:
                    _connection(_WorkspaceTarget(ssh, bridge, ssh_destination(ssh)))

        return _apply_locked(action, host, path, target, scopes, homes, registry_path=registry_path, scan=scan,
                             recheck=recheck, check_bridge=check_bridge, bridge_sync_pending=sync_pending)
