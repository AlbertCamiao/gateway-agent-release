"""Open a read-only Codex App Server connection on an approved SSH host."""

import asyncio
import os
import re
import shlex
import socket
import stat
import subprocess
import threading
import time
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import UUID

from feishu_gateway.executors.codex_app_server import PreviewError, _AppServer, _close_server
from feishu_gateway.catalog.types import INTERACTIVE_SOURCES, SessionCandidate
from feishu_gateway.contracts import SessionRef


_HOST_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]{0,252}\Z")
_READ_METHODS = frozenset({
    "initialize",
    "initialized",
    "skills/list",
    "thread/list",
    "thread/read",
    "thread/resume",
    "turn/start",
    "thread/turns/list",
    "thread/items/list",
})
_SSH_OPTIONS = (
    "BatchMode=yes",
    "ConnectTimeout=8",
    "ConnectionAttempts=1",
    "StrictHostKeyChecking=yes",
    "UpdateHostKeys=no",
    "ServerAliveInterval=5",
    "ServerAliveCountMax=2",
    "ForwardAgent=no",
    "ForwardX11=no",
    "PermitLocalCommand=no",
    "RemoteCommand=none",
    "ClearAllForwardings=yes",
    "ControlMaster=no",
    "ControlPersist=no",
)
_CHECK_PATH = """
import sys
from pathlib import Path

try:
    for value in sys.argv[1:]:
        path = Path(value)
        if path.resolve(strict=True) != path or not path.is_dir():
            sys.exit(3)
except (OSError, RuntimeError, ValueError):
    sys.exit(3)
"""


def canonical_remote_cwd(cwd: object) -> PurePosixPath | None:
    if not isinstance(cwd, str) or not cwd.isprintable():
        return None
    path = PurePosixPath(cwd)
    if path.root != "/" or not path.name or ".." in path.parts or str(path) != cwd:
        return None
    return path


def is_workspace_cwd(cwd: str | PurePosixPath | Path, roots: Collection[PurePosixPath]) -> bool:
    """Match a canonical remote path to a root or a real path-component descendant."""
    path = canonical_remote_cwd(str(cwd))
    return path is not None and any(
        canonical_remote_cwd(str(root)) is not None and path.is_relative_to(root) for root in roots
    )


def _socket_is_live(path: Path) -> bool:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(0.75)
    try:
        client.connect(str(path))
        return True
    except OSError:
        return False
    finally:
        client.close()


def _owner_socket_entry(path: Path) -> os.stat_result | None:
    """Validate a forward path before probing, replacing, or trusting it."""
    try:
        parent = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
        ):
            raise PreviewError("owner_unavailable", "本地 owner socket 目录必须是当前用户的私有 0700 目录。")
        try:
            entry = path.lstat()
        except FileNotFoundError:
            return None
    except OSError:
        raise PreviewError("owner_unavailable", "无法检查本地 owner socket。") from None
    if (
        not stat.S_ISSOCK(entry.st_mode)
        or entry.st_uid != os.getuid()
        or stat.S_IMODE(entry.st_mode) & 0o077
    ):
        raise PreviewError("owner_unavailable", "本地 owner socket 不是当前用户的私有 socket。")
    return entry


def _remove_owner_socket(path: Path) -> None:
    if _owner_socket_entry(path) is None:
        return
    try:
        path.unlink()
    except OSError:
        raise PreviewError("owner_unavailable", "无法清理陈旧 owner socket。") from None


_FORWARD_LOCKS: dict[Path, threading.Lock] = {}


def _forward_lock(path: Path) -> threading.Lock:
    # Feishu ingress and the daemon RPC path may execute on different asyncio
    # loops. A threading lock serializes the shared SSH forward without tying
    # the operation to whichever loop happened to create it first.
    lock = _FORWARD_LOCKS.get(path)
    if lock is None:
        lock = threading.Lock()
        _FORWARD_LOCKS[path] = lock
    return lock


class _ReadOnlyAppServer(_AppServer):
    async def send(self, message: dict) -> None:
        if message.get("method") not in _READ_METHODS:
            raise PreviewError("remote_read_only", "远端读取连接只允许查询。")
        await super().send(message)


_GATEWAY_METHODS = _READ_METHODS | {
    "thread/queue/add", "thread/queue/start", "thread/queue/list",
}


class _GatewayAppServer(_AppServer):
    async def send(self, message: dict) -> None:
        if message.get("method") not in _GATEWAY_METHODS:
            raise PreviewError("remote_read_only", "远端连接不允许该操作。")
        await super().send(message)


@dataclass(frozen=True, slots=True)
class SSHReadTransport:
    host: str
    remote_codex: str = "codex"
    control_path: Path | None = None
    ssh_alias: str | None = None
    app_server_socket: PurePosixPath | None = None
    allow_submit: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not _HOST_ALIAS.fullmatch(self.host):
            raise ValueError("SSH 主机须为已配置的主机别名。")
        if self.ssh_alias is not None and (
            not isinstance(self.ssh_alias, str) or not _HOST_ALIAS.fullmatch(self.ssh_alias)
        ):
            raise ValueError("SSH 主机须为已配置的主机别名。")
        if not isinstance(self.remote_codex, str) or not self.remote_codex.isprintable():
            raise ValueError("远端 Codex 命令无效。")
        executable = PurePosixPath(self.remote_codex)
        if self.remote_codex != "codex" and (not executable.is_absolute() or not executable.name):
            raise ValueError("远端 Codex 须为 codex 或绝对路径。")
        if self.control_path is not None:
            if not (
                isinstance(self.control_path, Path)
                and self.control_path.is_absolute()
                and str(self.control_path).isprintable()
                and "%" not in str(self.control_path)
            ):
                raise ValueError("SSH 控制套接字路径须为绝对路径。")

    def _ssh_command(self, remote_command: str) -> list[str]:
        command = ["/usr/bin/ssh", "-T"]
        for option in _SSH_OPTIONS:
            command.extend(("-o", option))
        if self.control_path is not None:
            # Multiplex the authenticated session. A closed master must not fall
            # back to an independent login between the check and SSH launch.
            command.extend(("-S", str(self.control_path), "-o", "ProxyCommand=/usr/bin/false"))
        command.extend(("--", self.ssh_alias or self.host, remote_command))
        return command

    def _launcher(self) -> str:
        if self.remote_codex != "codex":
            return f"exec {shlex.quote(self.remote_codex)}"
        # Non-interactive SSH often lacks ~/.local/bin in PATH. Resolve the
        # conventional Codex install locations in the remote login shell
        # instead of requiring a host-specific absolute path in config.
        return (
            'if [ -x "$HOME/.local/bin/codex" ]; then CODEX="$HOME/.local/bin/codex"; '
            'elif [ -x "$HOME/bin/codex" ]; then CODEX="$HOME/bin/codex"; '
            'elif command -v codex >/dev/null 2>&1; then CODEX="$(command -v codex)"; '
            'else CODEX=codex; fi; exec "$CODEX"'
        )

    def _command(self) -> list[str]:
        if self.app_server_socket is not None:
            socket = shlex.quote(str(self.app_server_socket))
            return self._ssh_command(
                f"{self._launcher()} app-server proxy --sock {socket}"
            )
        return self._ssh_command(f"{self._launcher()} app-server --listen stdio://")

    def _check_control_path(self) -> None:
        if self.control_path is None:
            return
        try:
            parent = self.control_path.parent.stat()
            socket = self.control_path.lstat()
        except OSError:
            raise PreviewError("remote_unavailable", "远端 SSH 登录会话已断开。") from None
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.getuid()
            or parent.st_mode & 0o077
            or not stat.S_ISSOCK(socket.st_mode)
            or socket.st_uid != os.getuid()
            or socket.st_mode & 0o077
        ):
            raise PreviewError("remote_unavailable", "远端 SSH 控制套接字不是当前用户的私有套接字。")

    def _ssh_environment(self) -> dict[str, str]:
        return {
            name: os.environ[name]
            for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
            if name in os.environ
        }

    def _control_alive(self) -> bool:
        if self.control_path is None:
            return True
        try:
            result = subprocess.run(
                ["/usr/bin/ssh", "-S", str(self.control_path), "-O", "check",
                 self.ssh_alias or self.host],
                env=self._ssh_environment(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=5, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0

    def _can_open_session(self) -> bool:
        """A live master can still refuse new multiplexed sessions."""
        if self.control_path is None:
            return True
        try:
            result = subprocess.run(
                [
                    "/usr/bin/ssh", "-T", "-S", str(self.control_path),
                    "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                    "-o", "ProxyCommand=/usr/bin/false",
                    "--", self.ssh_alias or self.host, "true",
                ],
                env=self._ssh_environment(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=12, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0

    def ensure_control_master(self) -> bool:
        """Recreate the authenticated ControlMaster when it cannot open sessions."""
        if self.control_path is None:
            return True
        if self._control_alive() and self._can_open_session():
            return True
        if self._control_alive():
            self.close_control_master()
        parent = self.control_path.parent
        try:
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            # Never chmod a shared system directory such as /tmp.
            if parent not in {Path("/"), Path("/tmp")}:
                parent.chmod(0o700)
            try:
                entry = self.control_path.lstat()
            except FileNotFoundError:
                pass
            else:
                if not stat.S_ISSOCK(entry.st_mode) or entry.st_uid != os.getuid():
                    return False
                self.control_path.unlink()
            command = [
                "/usr/bin/ssh", "-MNf", "-S", str(self.control_path),
                "-o", "ControlMaster=yes", "-o", "ControlPersist=yes",
                "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2",
                "--", self.ssh_alias or self.host,
            ]
            result = subprocess.run(
                command, env=self._ssh_environment(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=15, check=False,
            )
            if result.returncode != 0:
                return False
            for _ in range(10):
                if self._control_alive():
                    return True
                time.sleep(0.2)
        except (OSError, subprocess.SubprocessError, TimeoutError):
            return False
        return False

    async def ensure_owner_forward(
        self, local_socket: Path, remote_socket: PurePosixPath | None = None,
        *, force: bool = False,
    ) -> None:
        if self.control_path is None:
            return
        lock = _forward_lock(self.control_path)
        await asyncio.to_thread(lock.acquire)
        try:
            await self._ensure_owner_forward_locked(local_socket, remote_socket, force=force)
        finally:
            lock.release()

    async def _ensure_owner_forward_locked(
        self, local_socket: Path, remote_socket: PurePosixPath | None = None,
        *, force: bool = False,
    ) -> None:
        """Ensure the local Unix socket is a live SSH streamlocal forward.

        The remote Codex host replaces ~/.codex/ipc/ipc.sock when its UI/app
        server restarts. A local forwarded socket can survive that restart as
        an orphan; owner discovery then fails with owner_unavailable. Repair it
        from the existing authenticated ControlMaster, never with a new login.
        """
        if self.control_path is None:
            return
        if not local_socket.is_absolute() or "%" in str(local_socket):
            raise PreviewError("owner_unavailable", "Owner socket path is invalid.")
        entry = await asyncio.to_thread(_owner_socket_entry, local_socket)
        was_alive = await asyncio.to_thread(self._control_alive)
        if not await asyncio.to_thread(self.ensure_control_master):
            raise PreviewError("remote_unavailable", "远端 SSH 登录会话已断开。")
        # A newly created ControlMaster has no forwards. Never trust an orphan
        # local socket left by the previous connection.
        force = force or not was_alive
        if not force and entry is not None:
            if await asyncio.to_thread(_socket_is_live, local_socket):
                return
        if remote_socket is None:
            home = await asyncio.to_thread(self._remote_home)
            remote_socket = PurePosixPath(home) / ".codex" / "ipc" / "ipc.sock"
        if not remote_socket.is_absolute() or ".." in remote_socket.parts:
            raise PreviewError("owner_unavailable", "Remote owner socket path is invalid.")
        await asyncio.to_thread(_remove_owner_socket, local_socket)
        forward = f"{local_socket}:{remote_socket}"
        cancel = [
            "/usr/bin/ssh", "-S", str(self.control_path), "-O", "cancel",
            "-L", forward, "--", self.ssh_alias or self.host,
        ]
        command = [
            "/usr/bin/ssh", "-S", str(self.control_path), "-O", "forward",
            "-L", forward, "--", self.ssh_alias or self.host,
        ]
        try:
            await asyncio.to_thread(
                subprocess.run, cancel, env=self._ssh_environment(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=15, check=False,
            )
            result = await asyncio.to_thread(
                subprocess.run, command, env=self._ssh_environment(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=15, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            raise PreviewError("remote_unavailable", "无法建立远端 owner 转发。") from None
        ready = await asyncio.to_thread(_owner_socket_entry, local_socket)
        if result.returncode != 0 or ready is None:
            # A ControlMaster can look alive while its transport is stale.
            # Recreate it once and retry the same authenticated forward.
            await asyncio.to_thread(self.close_control_master)
            await asyncio.to_thread(_remove_owner_socket, local_socket)
            if not await asyncio.to_thread(self.ensure_control_master):
                raise PreviewError("remote_unavailable", "远端 SSH 登录会话已断开。")
            try:
                await asyncio.to_thread(
                    subprocess.run, cancel, env=self._ssh_environment(),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=15, check=False,
                )
                retry = await asyncio.to_thread(
                    subprocess.run, command, env=self._ssh_environment(),
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    timeout=15, check=False,
                )
            except (OSError, subprocess.SubprocessError):
                raise PreviewError("remote_unavailable", "无法建立远端 owner 转发。") from None
            ready = await asyncio.to_thread(_owner_socket_entry, local_socket)
            if retry.returncode != 0 or ready is None:
                raise PreviewError("owner_unavailable", "无法建立远端 Codex owner 转发。")

    def _remote_home(self) -> str:
        command = self._ssh_command('printf %s "$HOME"')
        try:
            result = subprocess.run(
                command, env=self._ssh_environment(),
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                timeout=8, check=False, text=True,
            )
        except (OSError, subprocess.SubprocessError):
            raise PreviewError("remote_unavailable", "无法读取远端 home。") from None
        home = result.stdout.strip()
        if result.returncode != 0 or not home.startswith("/") or "\n" in home:
            raise PreviewError("remote_unavailable", "远端 home 无效。")
        return home

    def close_control_master(self) -> None:
        """Close the master so a stopped Gateway leaves no SSH connection behind."""
        if self.control_path is None:
            return
        try:
            subprocess.run(
                ["/usr/bin/ssh", "-S", str(self.control_path), "-O", "exit",
                 self.ssh_alias or self.host],
                env=self._ssh_environment(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=5, check=False,
            )
        except (OSError, subprocess.SubprocessError, TimeoutError):
            pass

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_ReadOnlyAppServer]:
        """Yield a JSON-RPC client. The caller bounds the full read operation."""
        if self.control_path is not None:
            lock = _forward_lock(self.control_path)
            await asyncio.to_thread(lock.acquire)
            try:
                if not await asyncio.to_thread(self.ensure_control_master):
                    raise PreviewError("remote_unavailable", "远端 SSH 登录会话已断开。")
            finally:
                lock.release()
        self._check_control_path()
        # Keep local application credentials out of SSH's environment, including
        # when the user's SSH configuration defines SendEnv.
        environment = {
            name: os.environ[name]
            for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
            if name in os.environ
        }
        try:
            process = await asyncio.create_subprocess_exec(
                *self._command(),
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=2**20,
            )
        except OSError:
            raise PreviewError("remote_unavailable", "无法建立远端 Codex 读取连接。") from None
        try:
            yield _GatewayAppServer(process) if self.allow_submit else _ReadOnlyAppServer(process)
        finally:
            await _close_server(process)

    async def check_workspace_cwd(
        self, cwd: str | PurePosixPath | Path, roots: Collection[PurePosixPath],
    ) -> None:
        """Verify the owner path and its configured root resolve exactly on the host."""
        path = canonical_remote_cwd(str(cwd))
        root = next((root for root in roots if path is not None and is_workspace_cwd(path, (root,))), None)
        if root is None:
            raise PreviewError("session_unavailable", "远端会话不在授权工作区。")
        self._check_control_path()
        command = "exec " + shlex.join(("python3", "-I", "-c", _CHECK_PATH, str(root), str(path)))
        process = None
        try:
            async with asyncio.timeout(8):
                process = await asyncio.create_subprocess_exec(
                    *self._ssh_command(command), stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                    env={key: os.environ[key] for key in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
                         if key in os.environ},
                )
                code = await process.wait()
        except (OSError, TimeoutError):
            raise PreviewError("remote_unavailable", "无法核对远端会话的工作目录。") from None
        finally:
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
        if code == 3:
            raise PreviewError("session_unavailable", "远端会话的工作目录已变更或包含符号链接。")
        if code != 0:
            raise PreviewError("remote_unavailable", "无法核对远端会话的工作目录。")


async def discover_remote_sessions(
    transport: SSHReadTransport, *, project_root: PurePosixPath, timeout: float = 30,
) -> tuple[SessionCandidate, ...]:
    """Discover only interactive root sessions in the approved remote directory."""
    if not project_root.is_absolute() or ".." in project_root.parts or project_root == PurePosixPath("/"):
        raise PreviewError("catalog_scope", "远端会话目录必须是授权项目的绝对路径。")
    return await _discover_remote_sessions(
        transport, exact_roots=(project_root,), workspace_roots=(), cwd_filter=project_root, timeout=timeout,
    )


async def discover_remote_workspace_sessions(
    transport: SSHReadTransport, *, workspace_roots: Collection[PurePosixPath],
    exact_roots: Collection[PurePosixPath] = (), timeout: float = 30,
) -> tuple[SessionCandidate, ...]:
    """Query one remote inventory and project only authorized exact and workspace paths."""
    if not workspace_roots or any(
        canonical_remote_cwd(str(root)) is None for root in (*exact_roots, *workspace_roots)
    ):
        raise PreviewError("catalog_scope", "远端工作区必须使用规范的绝对路径。")
    return await _discover_remote_sessions(
        transport, exact_roots=exact_roots, workspace_roots=workspace_roots, cwd_filter=None, timeout=timeout,
    )


async def _discover_remote_sessions(
    transport: SSHReadTransport, *, exact_roots: Collection[PurePosixPath],
    workspace_roots: Collection[PurePosixPath], cwd_filter: PurePosixPath | None, timeout: float,
) -> tuple[SessionCandidate, ...]:
    candidates = {}
    seen_cursors = set()
    try:
        async with asyncio.timeout(timeout):
            async with transport.connect() as app:
                await app.call(
                    "initialize", {"clientInfo": {"name": "feishu_gateway_remote_catalog", "version": "0.1"}},
                )
                await app.send({"method": "initialized"})
                params = {
                    "archived": False,
                    "limit": 50,
                    "sourceKinds": INTERACTIVE_SOURCES,
                    "useStateDbOnly": True,
                }
                if cwd_filter is not None:
                    params["cwd"] = str(cwd_filter)
                while True:
                    page = await app.call("thread/list", params)
                    for thread in page["data"]:
                        raw_cwd = thread.get("cwd")
                        if cwd_filter is not None and raw_cwd == str(cwd_filter):
                            cwd = cwd_filter
                        else:
                            cwd = canonical_remote_cwd(raw_cwd)
                        if (
                            cwd is None or (cwd not in exact_roots and not is_workspace_cwd(cwd, workspace_roots))
                            or thread.get("source") not in INTERACTIVE_SOURCES
                            or thread.get("ephemeral") is not False
                            or thread.get("parentThreadId") is not None
                            or thread.get("archived", False) is not False
                        ):
                            continue
                        thread_id = thread.get("id")
                        try:
                            if not isinstance(thread_id, str) or str(UUID(thread_id)) != thread_id:
                                continue
                        except ValueError:
                            continue
                        title = thread.get("name")
                        if not isinstance(title, str) or not title.strip() or not title.isprintable():
                            title = None
                        target = SessionRef(transport.host, "codex", thread_id)
                        previous = candidates.get(thread_id)
                        if previous is not None and previous.cwd != cwd:
                            raise PreviewError("catalog_changed", "远端 Codex 会话目录的工作目录发生变化。")
                        candidates.setdefault(thread_id, SessionCandidate(target, title, cwd))

                    cursor = page.get("nextCursor")
                    if cursor is None:
                        return tuple(candidates.values())
                    if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                        raise PreviewError("catalog_protocol", "远端会话目录返回了无效的分页游标。")
                    seen_cursors.add(cursor)
                    params["cursor"] = cursor
    except (OSError, TimeoutError):
        raise PreviewError("catalog_unavailable", "远端 Codex 会话目录暂不可用。") from None
    except (KeyError, TypeError, AttributeError, ValueError) as error:
        import logging
        logging.getLogger("feishu-gateway").warning(
            "Remote Codex catalog protocol error: %r", error, exc_info=True,
        )
        raise PreviewError("catalog_protocol", "远端 Codex 会话目录返回了无效的数据。") from None
