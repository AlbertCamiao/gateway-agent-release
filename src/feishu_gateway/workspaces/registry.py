"""Persist exact workspace permissions separately from host connections."""

import fcntl
import json
import os
import re
import stat
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile

from feishu_gateway.config import REGISTRY_PATH


_HOST = re.compile(r'[a-z0-9][a-z0-9._-]{0,63}')
_MAX_SIZE = 1024 * 1024


class WorkspaceRegistryError(ValueError):
    pass


def absolute_workspace(raw):
    if not isinstance(raw, str) or not raw.isprintable() or len(raw.encode()) > 1024:
        raise WorkspaceRegistryError('Invalid workspace path.')
    path = PurePosixPath(raw)
    if not path.is_absolute() or not path.name or '..' in path.parts or str(path) != raw or raw.startswith('//'):
        raise WorkspaceRegistryError('Use a canonical absolute workspace path.')
    return path


def _private_fd(path, flags):
    fd = os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
        os.close(fd)
        raise WorkspaceRegistryError('Invalid registry file permissions.')
    return fd


def read_document(path=None):
    path = REGISTRY_PATH if path is None else Path(path)
    try:
        fd = _private_fd(path, os.O_RDONLY)
    except FileNotFoundError:
        return {}, {}
    except OSError as error:
        raise WorkspaceRegistryError('Could not open workspace registry.') from error
    try:
        with os.fdopen(fd, 'rb') as stream:
            data = stream.read(_MAX_SIZE + 1)
        if len(data) > _MAX_SIZE:
            raise ValueError
        document = json.loads(data)
        if (not isinstance(document, dict) or set(document) != {'version', 'hosts'}
                or type(document['version']) is not int or document['version'] != 1
                or not isinstance(document['hosts'], dict)):
            raise ValueError
        scopes, homes = {}, {}
        for name, entry in document['hosts'].items():
            if (not _HOST.fullmatch(name) or not isinstance(entry, dict) or set(entry) != {'home', 'workspaces'}
                    or not isinstance(entry['workspaces'], list)):
                raise ValueError
            homes[name] = absolute_workspace(entry['home'])
            roots = tuple(absolute_workspace(value) for value in entry['workspaces'])
            if len(set(roots)) != len(roots) or not roots:
                raise ValueError
            scopes[name] = roots
        return scopes, homes
    except (ValueError, OSError, TypeError, UnicodeError) as error:
        raise WorkspaceRegistryError('Invalid workspace registry.') from error


def read_registry(path=None):
    return read_document(path)[0]


def write_registry(scopes, homes, path=None):
    path = REGISTRY_PATH if path is None else Path(path)
    document = {'version': 1, 'hosts': {
        host: {'home': str(homes[host]), 'workspaces': [str(root) for root in sorted(roots)]}
        for host, roots in sorted(scopes.items()) if roots
    }}
    data = json.dumps(document, ensure_ascii=False, indent=2).encode() + b'\n'
    if len(data) > _MAX_SIZE:
        raise WorkspaceRegistryError('Workspace registry is too large.')
    with NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        staged = Path(stream.name)
        try:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            staged.replace(path)
        finally:
            staged.unlink(missing_ok=True)


def _lock_file(path):
    path = REGISTRY_PATH if path is None else Path(path)
    return os.fdopen(_private_fd(path.with_suffix('.lock'), os.O_RDWR | os.O_CREAT), 'rb+')


@contextmanager
def registry_lock(path=None):
    with _lock_file(path) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield
