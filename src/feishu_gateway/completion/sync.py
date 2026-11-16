"""Sync the local completion endpoint into a remote Codex/DSH hook config."""

from __future__ import annotations

import base64
import json
import os
import shlex
import pathlib
import subprocess
from feishu_gateway import config
from feishu_gateway.workspaces.target import host_for


_REMOTE = r'''
import json, os, pathlib, sys
data = json.load(sys.stdin)
home = pathlib.Path.home()
def write(path, text):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(text)
        stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)
    os.chmod(path, 0o600)
write(home / ".codex" / "hooks" / ".feishu-port", str(data["port"]))
write(home / ".codex" / "hooks" / ".feishu-token", data["token"])
write(home / ".codex" / "hooks" / ".feishu-host", data["host"])
if data.get("dsh"):
    write(
        home / ".dsh" / "gateway-bridge" / "completion.json",
        json.dumps({
            "url": f"http://127.0.0.1:{data['port']}/completion",
            "token": data["token"],
            "host": data["host"],
        }, separators=(",", ":")),
    )
print(json.dumps({"ok": True}))
'''


def _local_endpoint() -> tuple[int, str]:
    data = json.loads(config.COMPLETION_STATE_PATH.read_text())
    port = data.get("port")
    token = data.get("token")
    if not isinstance(port, int) or not 1024 <= port <= 65535:
        raise ValueError("completion port is invalid")
    if not isinstance(token, str) or not 32 <= len(token) <= 256:
        raise ValueError("completion token is invalid")
    return port, token


def _write_local(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    os.chmod(path, 0o600)


def _sync_local(port: int, token: str, *, dsh: bool) -> dict:
    import pathlib
    from feishu_gateway.completion.codex_hook import install_local_hook

    hooks = pathlib.Path.home() / ".codex" / "hooks"
    _write_local(hooks / ".feishu-port", str(port))
    _write_local(hooks / ".feishu-token", token)
    _write_local(hooks / ".feishu-host", "local")
    install_local_hook()
    if dsh:
        _write_local(
            pathlib.Path.home() / ".dsh" / "gateway-bridge" / "completion.json",
            json.dumps({
                "url": f"http://127.0.0.1:{port}/completion",
                "token": token,
                "host": "local",
            }, separators=(",", ":")),
        )
    return {"host": "local", "port": port, "dsh": dsh, "tunnel": "local"}


def sync_endpoint(host: str, *, dsh: bool = True) -> dict:
    port, token = _local_endpoint()
    scopes = config.read_ssh_scopes(config.SSH_CONFIG_PATH)
    resolved, transport = host_for(host, scopes)
    if transport is None:
        return _sync_local(port, token, dsh=bool(dsh))
    if transport.control_path is None:
        raise ValueError("remote SSH transport is unavailable")
    if not transport.ensure_control_master():
        raise ValueError("SSH ControlMaster is unavailable")
    payload = {"host": resolved, "port": port, "token": token, "dsh": bool(dsh)}
    encoded = base64.b64encode(_REMOTE.encode()).decode()
    command = transport._ssh_command(
        "exec python3 -I -c " + shlex.quote(
            f"import base64; exec(base64.b64decode({encoded!r}))"
        )
    )
    env = {
        name: os.environ[name] for name in ("HOME", "PATH", "USER", "LOGNAME", "SSH_AUTH_SOCK")
        if name in os.environ
    }
    result = subprocess.run(
        command, input=json.dumps(payload), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False, env=env,
    )
    if result.returncode != 0:
        detail = (result.stderr or "").strip().splitlines()[-1:] or ["remote endpoint sync failed"]
        raise ValueError(detail[0][:200])
    try:
        response = json.loads(result.stdout)
    except ValueError:
        raise ValueError("remote endpoint sync returned invalid data") from None
    if response.get("ok") is not True:
        raise ValueError("remote endpoint sync was rejected")
    forward = subprocess.run(
        ["/usr/bin/ssh", "-S", str(transport.control_path), "-O", "forward",
         "-R", f"127.0.0.1:{port}:127.0.0.1:{port}", resolved],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15, check=False, env=env,
    )
    if forward.returncode != 0:
        raise ValueError("completion reverse tunnel failed")
    return {"host": resolved, "port": port, "dsh": bool(dsh), "tunnel": "forwarded"}
