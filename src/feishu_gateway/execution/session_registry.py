"""Durable facts about sessions observed on native session hosts.

The registry is not an execution owner and never creates or resumes a session.
It records what a catalog read observed, so a temporary catalog/owner failure
does not erase an otherwise known live session.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from collections.abc import Iterable, Mapping
from pathlib import Path


class SessionRegistry:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._db = sqlite3.connect(
            str(self.path), timeout=5.0, check_same_thread=False,
        )
        self._lock = threading.RLock()
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS session_registry (
                session_id TEXT NOT NULL,
                host TEXT NOT NULL,
                runtime TEXT NOT NULL,
                workspace TEXT NOT NULL,
                title TEXT,
                state TEXT,
                live INTEGER NOT NULL DEFAULT 1,
                injectable INTEGER NOT NULL DEFAULT 1,
                owner_state TEXT NOT NULL DEFAULT 'unknown',
                stale INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'catalog',
                last_seen REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (host, session_id)
            )
            """
        )
        self._db.commit()
        self._harden_files()

    def record(self, rows: Iterable[Mapping[str, object]], *, host: str | None = None) -> int:
        now = time.time()
        written = 0
        with self._lock, self._db:
            for row in rows:
                session_id = str(row.get("session_id") or "")
                session_host = str(host or row.get("host") or "")
                runtime = str(row.get("runtime") or "")
                workspace = str(row.get("workspace") or row.get("cwd") or "")
                if not session_id or not session_host or not runtime or not workspace:
                    continue
                self._db.execute(
                    """
                    INSERT INTO session_registry
                        (session_id, host, runtime, workspace, title, state,
                         live, injectable, owner_state, stale, source, last_seen, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, 1, 'unknown', 0, 'catalog', ?, ?)
                    ON CONFLICT(host, session_id) DO UPDATE SET
                        runtime=excluded.runtime,
                        workspace=excluded.workspace,
                        title=excluded.title,
                        state=excluded.state,
                        live=1,
                        injectable=1,
                        owner_state='unknown',
                        stale=0,
                        source=excluded.source,
                        last_seen=excluded.last_seen,
                        updated_at=excluded.updated_at
                    """,
                    (
                        session_id, session_host, runtime, workspace,
                        row.get("title"), row.get("state"), now, now,
                    ),
                )
                written += 1
        self._harden_files()
        return written

    def mark_host_stale(self, host: str) -> int:
        with self._lock, self._db:
            cursor = self._db.execute(
                "UPDATE session_registry SET stale=1, updated_at=? WHERE host=?",
                (time.time(), host),
            )
        return cursor.rowcount

    def list_rows(self, *, host: str | None = None, include_stale: bool = True) -> list[dict]:
        query = "SELECT * FROM session_registry"
        clauses: list[str] = []
        params: list[object] = []
        if host is not None:
            clauses.append("host=?")
            params.append(host)
        if not include_stale:
            clauses.append("stale=0")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY last_seen DESC, session_id"
        with self._lock:
            return [self._row(row) for row in self._db.execute(query, params)]

    def get(self, host: str, session_id: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM session_registry WHERE host=? AND session_id=?",
                (host, session_id),
            ).fetchone()
        return self._row(row) if row is not None else None

    def _harden_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.path) + suffix)
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass

    def close(self) -> None:
        with self._lock:
            self._db.close()

    @staticmethod
    def _row(row: sqlite3.Row) -> dict:
        return {
            "session_id": row["session_id"],
            "host": row["host"],
            "runtime": row["runtime"],
            "workspace": row["workspace"],
            "cwd": row["workspace"],
            "title": row["title"],
            "state": row["state"],
            "live": bool(row["live"]),
            "injectable": bool(row["injectable"]),
            "owner_state": row["owner_state"],
            "stale": bool(row["stale"]),
            "source": row["source"],
            "last_seen": row["last_seen"],
        }
