"""Local ingress configuration, binding, and one-time challenge state."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import time
from pathlib import Path
from typing import Any


_APP_ID = re.compile(r"^cli_[A-Za-z0-9]{4,128}$")
_CHAT = re.compile(r"^oc_[A-Za-z0-9]{4,128}$")
_SENDER = re.compile(r"^ou_[A-Za-z0-9]{4,128}$")
_ENV_LINE = re.compile(r"^(FEISHU_APP_ID|FEISHU_APP_SECRET)=(.*)$")


class IngressError(ValueError):
    pass


def _write_private(path: Path, data: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _read_private(path: Path, limit: int = 65536) -> bytes:
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise IngressError("ingress state is too large")
    return data


def read_ingress(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(_read_private(path))
    except FileNotFoundError:
        return {"feishu": {"enabled": False, "profile": "web", "transport": "ws"}}
    except (ValueError, TypeError, OSError) as error:
        raise IngressError("ingress.json is invalid") from error
    if not isinstance(raw, dict) or set(raw) != {"feishu"} or not isinstance(raw["feishu"], dict):
        raise IngressError("ingress.json has an invalid shape")
    feishu = raw["feishu"]
    if not isinstance(feishu.get("enabled"), bool):
        raise IngressError("ingress.json enabled must be boolean")
    profile = feishu.get("profile", "web")
    transport = feishu.get("transport", "ws")
    if not isinstance(profile, str) or not profile or transport != "ws":
        raise IngressError("ingress.json profile/transport is invalid")
    return {"feishu": {"enabled": feishu["enabled"], "profile": profile, "transport": "ws"}}


def write_ingress(path: Path, *, enabled: bool, profile: str = "web") -> None:
    if not isinstance(enabled, bool) or not isinstance(profile, str) or not profile:
        raise IngressError("invalid ingress config")
    payload = {"feishu": {"enabled": enabled, "profile": profile, "transport": "ws"}}
    _write_private(path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode())


def read_credentials(env_path: Path) -> tuple[str | None, str | None]:
    try:
        text = _read_private(env_path).decode("utf-8")
    except FileNotFoundError:
        return None, None
    except (OSError, UnicodeError) as error:
        raise IngressError("could not read .env") from error
    values: dict[str, str] = {}
    for line in text.splitlines():
        match = _ENV_LINE.fullmatch(line.strip())
        if match:
            values[match.group(1)] = match.group(2).strip().strip('"').strip("'")
    return values.get("FEISHU_APP_ID"), values.get("FEISHU_APP_SECRET")


def write_credentials(env_path: Path, app_id: str, app_secret: str) -> None:
    if not _APP_ID.fullmatch(app_id) or not app_secret or len(app_secret) > 512:
        raise IngressError("invalid Feishu app credentials")
    try:
        lines = _read_private(env_path).decode("utf-8").splitlines()
    except FileNotFoundError:
        lines = []
    result: list[str] = []
    seen: set[str] = set()
    for line in lines:
        match = _ENV_LINE.fullmatch(line.strip())
        if not match:
            result.append(line)
            continue
        key = match.group(1)
        if key in seen:
            continue
        seen.add(key)
        result.append(f"{key}={app_id if key == 'FEISHU_APP_ID' else app_secret}")
    for key in ("FEISHU_APP_ID", "FEISHU_APP_SECRET"):
        if key not in seen:
            result.append(f"{key}={app_id if key == 'FEISHU_APP_ID' else app_secret}")
    _write_private(env_path, ("\n".join(result).rstrip() + "\n").encode())


def read_binding(path: Path, *, app_id: str | None = None) -> dict[str, Any] | None:
    try:
        raw = json.loads(_read_private(path))
    except FileNotFoundError:
        return None
    except (ValueError, TypeError, OSError) as error:
        raise IngressError("binding state is invalid") from error
    if not isinstance(raw, dict):
        raise IngressError("binding state is invalid")
    if app_id is not None and raw.get("app_id") != app_id:
        return None
    if not _APP_ID.fullmatch(str(raw.get("app_id", ""))):
        return None
    if not _CHAT.fullmatch(str(raw.get("chat_id", ""))) or not _SENDER.fullmatch(str(raw.get("sender_id", ""))):
        return None
    return raw


def write_binding(
    path: Path, *, app_id: str, chat_id: str, sender_id: str,
    p2p_chat_id: str | None, chat_type: str,
) -> dict[str, Any]:
    if not _APP_ID.fullmatch(app_id) or not _CHAT.fullmatch(chat_id) or not _SENDER.fullmatch(sender_id):
        raise IngressError("invalid binding identity")
    if p2p_chat_id is not None and not _CHAT.fullmatch(p2p_chat_id):
        raise IngressError("invalid p2p chat identity")
    if chat_type not in {"group", "p2p"}:
        raise IngressError("invalid chat type")
    payload = {
        "version": 1, "app_id": app_id, "chat_id": chat_id, "sender_id": sender_id,
        "p2p_chat_id": p2p_chat_id, "chat_type": chat_type, "bound_at": time.time(),
    }
    _write_private(path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode())
    return payload


def _challenge_hash(code: str) -> str:
    return hashlib.sha256(f"gateway-ingress-bind:{code}".encode()).hexdigest()


def generate_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def issue_challenge(path: Path, code: str, *, ttl: float = 600.0, now: float | None = None) -> None:
    if not re.fullmatch(r"[0-9]{6}", code):
        raise IngressError("binding code must be six digits")
    current = time.time() if now is None else now
    payload = {
        "version": 1, "code_hash": _challenge_hash(code), "issued_at": current,
        "expires_at": current + ttl, "attempts_left": 5,
    }
    _write_private(path, (json.dumps(payload, separators=(",", ":")) + "\n").encode())


def consume_challenge(path: Path, code: str, *, now: float | None = None) -> bool:
    current = time.time() if now is None else now
    try:
        raw = json.loads(_read_private(path, limit=4096))
    except (FileNotFoundError, ValueError, TypeError, OSError):
        return False
    if not isinstance(raw, dict):
        return False
    expires_at = raw.get("expires_at")
    attempts = raw.get("attempts_left")
    if not isinstance(expires_at, (int, float)) or not isinstance(attempts, int) or attempts <= 0:
        path.unlink(missing_ok=True)
        return False
    if current > float(expires_at):
        path.unlink(missing_ok=True)
        return False
    if secrets.compare_digest(_challenge_hash(code), str(raw.get("code_hash", ""))):
        path.unlink(missing_ok=True)
        return True
    raw["attempts_left"] = attempts - 1
    if raw["attempts_left"] <= 0:
        path.unlink(missing_ok=True)
    else:
        _write_private(path, (json.dumps(raw, separators=(",", ":")) + "\n").encode())
    return False
