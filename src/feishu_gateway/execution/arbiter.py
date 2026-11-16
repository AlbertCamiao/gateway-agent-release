"""Durable single-flight leases per Gateway session."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

from feishu_gateway.contracts import SessionRef


_SCHEMA = """
CREATE TABLE IF NOT EXISTS session_leases (
    session_key TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL,
    owner_instance TEXT NOT NULL,
    acquired_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    expires_at REAL NOT NULL
)
"""


class SessionBusy(RuntimeError):
    def __init__(self, task_id: str) -> None:
        super().__init__("session is busy")
        self.task_id = task_id


def session_key(target: SessionRef) -> str:
    return f"{target.host}|{target.runtime}|{target.session_id}"


def _conn(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute(_SCHEMA)
    return connection


class SessionArbiter:
    def __init__(self, path: Path | str, *, default_ttl: float = 3600.0) -> None:
        self.path = Path(path)
        with _conn(self.path):
            pass
        os.chmod(self.path, 0o600)
        self.default_ttl = default_ttl

    def acquire(
        self, target: SessionRef, task_id: str, owner_instance: str,
        *, ttl: float | None = None, now: float | None = None,
    ) -> None:
        key = session_key(target)
        current = time.time() if now is None else now
        expiry = current + (self.default_ttl if ttl is None else ttl)
        with _conn(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT task_id, expires_at FROM session_leases WHERE session_key=?",
                    (key,),
                ).fetchone()
                if row is not None and float(row["expires_at"]) > current:
                    raise SessionBusy(str(row["task_id"]))
                connection.execute(
                    """INSERT INTO session_leases
                       (session_key, task_id, owner_instance, acquired_at, heartbeat_at, expires_at)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(session_key) DO UPDATE SET
                         task_id=excluded.task_id, owner_instance=excluded.owner_instance,
                         acquired_at=excluded.acquired_at, heartbeat_at=excluded.heartbeat_at,
                         expires_at=excluded.expires_at""",
                    (key, task_id, owner_instance, current, current, expiry),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise

    def release(self, target: SessionRef, task_id: str) -> None:
        with _conn(self.path) as connection:
            connection.execute(
                "DELETE FROM session_leases WHERE session_key=? AND task_id=?",
                (session_key(target), task_id),
            )

    def heartbeat(self, target: SessionRef, task_id: str, *, ttl: float | None = None,
                  now: float | None = None) -> bool:
        current = time.time() if now is None else now
        expiry = current + (self.default_ttl if ttl is None else ttl)
        with _conn(self.path) as connection:
            changed = connection.execute(
                "UPDATE session_leases SET heartbeat_at=?, expires_at=? "
                "WHERE session_key=? AND task_id=?",
                (current, expiry, session_key(target), task_id),
            ).rowcount
        return bool(changed)

    def lease(self, target: SessionRef) -> dict[str, object] | None:
        """Read the persisted lease row, including an expired one."""
        with _conn(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM session_leases WHERE session_key=?",
                (session_key(target),),
            ).fetchone()
        return dict(row) if row is not None else None

    def active(self, target: SessionRef, *, now: float | None = None) -> str | None:
        current = time.time() if now is None else now
        with _conn(self.path) as connection:
            row = connection.execute(
                "SELECT task_id, expires_at FROM session_leases WHERE session_key=?",
                (session_key(target),),
            ).fetchone()
        if row is None or float(row["expires_at"]) <= current:
            return None
        return str(row["task_id"])

    def active_all(self, *, now: float | None = None) -> list[dict[str, object]]:
        current = time.time() if now is None else now
        with _conn(self.path) as connection:
            rows = connection.execute(
                "SELECT session_key, task_id, owner_instance, expires_at "
                "FROM session_leases WHERE expires_at > ? ORDER BY acquired_at",
                (current,),
            ).fetchall()
        return [dict(row) for row in rows]
