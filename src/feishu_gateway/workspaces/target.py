"""Resolve and configure a workspace on a known local or SSH host."""

import json
import ipaddress
import os
import shlex
import subprocess
import sys
from pathlib import PurePosixPath

from feishu_gateway.workspaces.registry import WorkspaceRegistryError, absolute_workspace


def host_for(value, scopes):
    name = value.casefold()
    if name in {'local', '本机', '本地'}:
        return 'local', None
    for scope in scopes:
        if name in (scope.transport.host, *scope.aliases):
            return scope.transport.host, scope.transport
    matches = []
    for scope in scopes:
        ssh = scope.transport
        try:
            hostname, _, _ = ssh_destination(ssh)
        except WorkspaceRegistryError:
            continue
        if name == hostname.casefold():
            matches.append(ssh)
    if len(matches) == 1:
        return matches[0].host, matches[0]
    if matches:
        raise WorkspaceRegistryError('Address matches several SSH hosts. Use an SSH alias.')
    raise WorkspaceRegistryError(f'Unknown Gateway host: {value!r}.')


def ssh_destination(ssh):
    """Resolve the current SSH configuration without opening a connection."""
    try:
        result = subprocess.run(['/usr/bin/ssh', '-G', '--', ssh.ssh_alias or ssh.host],
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=4, check=True)
        fields = dict(line.split(maxsplit=1) for line in result.stdout.splitlines()
                      if line.startswith(('hostname ', 'user ', 'port ')))
        hostname, user, port = fields['hostname'], fields['user'], int(fields['port'])
        if not hostname.isprintable() or not user.isprintable() or not 1 <= port <= 65535:
            raise ValueError
        return hostname, user, port
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        raise WorkspaceRegistryError('Could not resolve the configured SSH destination.') from error


def verify_ssh_endpoint(ssh, destination):
    """Compare the established SSH peer with its configured address and user."""
    actual = _run_host(ssh, 'endpoint')
    hostname, user, port = destination
    try:
        if len(actual) != 3 or type(actual[1]) is not int or actual[1] != port or actual[2] != user:
            raise ValueError
        peer = ipaddress.ip_address(actual[0])
        try:
            address = ipaddress.ip_address(hostname)
        except ValueError:
            address = None
        if address is not None and peer != address:
            raise ValueError
    except (TypeError, ValueError) as error:
        raise WorkspaceRegistryError('SSH control connection does not match the configured endpoint.') from error
    return str(peer), port, user


def workspace_path(value, home):
    if not value or not value.isprintable() or len(value.encode()) > 1024 or value.startswith('//'):
        raise WorkspaceRegistryError('Invalid workspace path.')
    path = PurePosixPath(value)
    if '..' in path.parts:
        raise WorkspaceRegistryError('Workspace paths cannot contain parent traversal.')
    if path.is_absolute():
        return absolute_workspace(str(path))
    if not path.parts or path.parts[0] == '~':
        return home.joinpath(*path.parts[1:])
    if path.parts[0] == home.name:
        return home.parent / path
    if path.parts[0].startswith('~'):
        raise WorkspaceRegistryError('Use ~/ for the target account’s home.')
    return home / path


_HOST_TOOL = r'''
import json
import os
import pwd
import stat
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile


def private(info, kind, mode):
    return kind(info.st_mode) and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == mode


def existing_dir(value):
    directory = Path(value)
    if not directory.is_absolute() or directory.resolve(strict=True) != directory or not directory.is_dir():
        raise ValueError
    info = directory.stat()
    return [info.st_dev, info.st_ino]


def set_scope(directory, roots):
    directory = Path(directory)
    if not private(directory.lstat(), stat.S_ISDIR, 0o700):
        raise ValueError
    config = directory / 'bridge-workspaces.json'
    if not private(config.lstat(), stat.S_ISREG, 0o600):
        raise ValueError
    contents = json.dumps({'roots': [{'path': path} for path in roots]}, ensure_ascii=False).encode() + b'\n'
    if len(roots) > 32 or len(contents) > 32768:
        raise ValueError
    with NamedTemporaryFile(dir=directory, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
            temporary.replace(config)
        finally:
            temporary.unlink(missing_ok=True)


try:
    data = json.loads(sys.stdin.buffer.read(65537))
    operation = data['operation']
    result = True
    if operation == 'home':
        result = str(Path.home().resolve(strict=True))
    elif operation == 'endpoint':
        _, _, address, port = os.environ['SSH_CONNECTION'].split()
        result = [address, int(port), pwd.getpwuid(os.getuid()).pw_name]
    elif operation == 'check':
        result = existing_dir(data['workspace'])
    elif operation == 'bridge':
        set_scope(data['bridge'], data['roots'])
    else:
        raise ValueError
    print(json.dumps(result))
except (ValueError, OSError, TypeError, KeyError):
    raise SystemExit(1) from None
'''


def _run_host(ssh, operation, **arguments):
    args = [sys.executable if ssh is None else 'python3', '-I', '-c', _HOST_TOOL]
    if ssh is not None:
        ssh._check_control_path()
        args = ssh._ssh_command('exec ' + shlex.join(args))
    environment = {name: os.environ[name] for name in ('HOME', 'PATH', 'USER', 'LOGNAME') if name in os.environ}
    try:
        result = subprocess.run(args, input=json.dumps({'operation': operation, **arguments}),
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
                                env=environment, timeout=12, check=False)
        if result.returncode != 0 or len(result.stdout) > 4096:
            raise ValueError
        return json.loads(result.stdout)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        if operation == 'check':
            message = 'Workspace must be an existing canonical directory on the target host.'
        elif operation == 'bridge':
            message = 'Could not update the DSH workspace scope.'
        elif operation == 'endpoint':
            message = 'Could not verify the active SSH endpoint.'
        else:
            message = 'Could not read the target host’s home directory.'
        raise WorkspaceRegistryError(message) from error


def target_home(ssh):
    return absolute_workspace(_run_host(ssh, 'home'))


def check_workspace(ssh, path):
    identity = _run_host(ssh, 'check', workspace=str(path))
    if not isinstance(identity, list) or len(identity) != 2 or any(type(value) is not int for value in identity):
        raise WorkspaceRegistryError('Could not verify the target workspace.')
    return tuple(identity)


def update_dsh_roots(bridge, roots):
    if bridge is not None:
        _run_host(bridge.ssh, 'bridge', bridge=str(bridge.remote_dir), roots=[str(root) for root in sorted(roots)])
