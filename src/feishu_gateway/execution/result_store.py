"""Durable task result store for the no-Feishu completion path."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from feishu_gateway.persistence import connect as sqlite_connect, initialize_schema


_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_results (
    task_id TEXT PRIMARY KEY NOT NULL,
    request_id TEXT,
    origin TEXT NOT NULL,
    host TEXT,
    runtime TEXT,
    session_id TEXT,
    workspace TEXT,
    state TEXT NOT NULL,
    outcome TEXT,
    output TEXT,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL,
    notified_at REAL,
    notify_error TEXT
);
CREATE TABLE IF NOT EXISTS completion_event_claims (
    event_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS request_fingerprints (
    task_id TEXT PRIMARY KEY NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at REAL NOT NULL
)
"""


def _conn(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    connection = sqlite_connect(path)
    initialize_schema(connection, path, schema=_SCHEMA)
    return connection


class ResultStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        with _conn(self.path):
            pass
        os.chmod(self.path, 0o600)

    def upsert(
        self, task_id: str, *, origin: str, state: str,
        request_id: str | None = None, host: str | None = None,
        runtime: str | None = None, session_id: str | None = None,
        workspace: str | None = None, outcome: str | None = None,
        output: str | None = None, error: str | None = None,
        request_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        terminal = state in {"completed", "failed", "uncertain", "rejected", "resolved"}
        with _conn(self.path) as connection:
            connection.execute(
                """INSERT INTO task_results
                   (task_id, request_id, origin, host, runtime, session_id, workspace,
                    state, outcome, output, error, created_at, updated_at, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET
                     state=excluded.state, outcome=excluded.outcome, output=excluded.output,
                     error=excluded.error, updated_at=excluded.updated_at,
                     completed_at=excluded.completed_at, notify_error=NULL""",
                (
                    task_id, request_id, origin, host, runtime, session_id, workspace,
                    state, outcome, output, error, now, now, now if terminal else None,
                ),
            )
            if request_fingerprint is not None:
                connection.execute(
                    """INSERT INTO request_fingerprints (task_id, fingerprint, created_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(task_id) DO UPDATE SET fingerprint=excluded.fingerprint""",
                    (task_id, request_fingerprint, now),
                )
        return self.get(task_id) or {}

    def transition(self, task_id: str, expected: str, state: str, **fields: Any) -> bool:
        """Compare-and-set one business state transition atomically."""
        allowed = {
            "origin", "request_id", "host", "runtime", "session_id",
            "workspace", "outcome", "output", "error",
        }
        if set(fields) - allowed:
            raise ValueError("unsupported task transition field")
        now = time.time()
        terminal = state in {"completed", "failed", "uncertain", "rejected", "resolved"}
        with _conn(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if expected == "new":
                values = {
                    "task_id": task_id, "origin": "cli", "state": state,
                    "created_at": now, "updated_at": now,
                    "completed_at": now if terminal else None,
                }
                values.update(fields)
                columns = ", ".join(values)
                placeholders = ", ".join("?" for _ in values)
                cursor = connection.execute(
                    f"INSERT INTO task_results ({columns}) VALUES ({placeholders}) "
                    "ON CONFLICT(task_id) DO NOTHING",
                    tuple(values.values()),
                )
            else:
                assignments = ["state=?", "updated_at=?"]
                params: list[Any] = [state, now]
                if terminal:
                    assignments.append("completed_at=?")
                    params.append(now)
                for key, value in fields.items():
                    assignments.append(f"{key}=?")
                    params.append(value)
                params.extend((task_id, expected))
                cursor = connection.execute(
                    f"UPDATE task_results SET {', '.join(assignments)} "
                    "WHERE task_id=? AND state=?",
                    tuple(params),
                )
            return cursor.rowcount == 1

    def request_fingerprint(self, task_id: str) -> str | None:
        with _conn(self.path) as connection:
            row = connection.execute(
                "SELECT fingerprint FROM request_fingerprints WHERE task_id=?",
                (task_id,),
            ).fetchone()
        return str(row["fingerprint"]) if row is not None else None

    def get(self, task_id: str) -> dict[str, Any] | None:
        with _conn(self.path) as connection:
            row = connection.execute(
                "SELECT * FROM task_results WHERE task_id=?", (task_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with _conn(self.path) as connection:
            rows = connection.execute(
                "SELECT * FROM task_results ORDER BY updated_at DESC LIMIT ?", (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_completion_event(
        self, event_id: str, task_id: str, *, now: float | None = None,
    ) -> str:
        """Durably bind one completion event id to one task.

        Returns new, pending (same task, retryable), completed (same task,
        ignore), or conflict (already bound to another task).
        """
        if not isinstance(event_id, str) or not event_id or len(event_id.encode()) > 512:
            raise ValueError("invalid completion event id")
        if not isinstance(task_id, str) or not task_id:
            raise ValueError("invalid completion task id")
        current = time.time() if now is None else now
        with _conn(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT task_id, status FROM completion_event_claims WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is not None:
                if str(row["task_id"]) != task_id:
                    return "conflict"
                return "completed" if row["status"] == "completed" else "pending"
            connection.execute(
                """INSERT INTO completion_event_claims
                   (event_id, task_id, status, created_at, updated_at)
                   VALUES (?, ?, 'pending', ?, ?)""",
                (event_id, task_id, current, current),
            )
        return "new"

    def complete_completion_event(self, event_id: str, *, now: float | None = None) -> None:
        current = time.time() if now is None else now
        with _conn(self.path) as connection:
            connection.execute(
                "UPDATE completion_event_claims SET status='completed', updated_at=? "
                "WHERE event_id=?",
                (current, event_id),
            )

    def release_completion_event(self, event_id: str) -> None:
        with _conn(self.path) as connection:
            connection.execute(
                "DELETE FROM completion_event_claims WHERE event_id=? AND status='pending'",
                (event_id,),
            )

    def claim(
        self, task_id: str, *, origin: str, request_id: str | None,
        host: str, runtime: str, session_id: str, workspace: str,
        now: float | None = None, recent_since: float | None = None,
        request_fingerprint: str | None = None,
    ) -> dict[str, Any] | None:
        """Atomically claim a target for a new task.

        Returns the inserted submitted row, or None when the task id or an
        unfinished task on the same target already exists. BEGIN IMMEDIATE
        serializes competing writers so a later write cannot overwrite the
        winner (N-22).
        """
        current = time.time() if now is None else now
        with _conn(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT task_id FROM task_results WHERE task_id = ?", (task_id,),
            ).fetchone()
            if existing is not None:
                return None
            active = connection.execute(
                """SELECT task_id FROM task_results
                   WHERE host=? AND runtime=? AND session_id=?
                     AND state IN ('submitted','running','uncertain')
                     AND (? IS NULL OR created_at >= ?)
                   LIMIT 1""",
                (host, runtime, session_id, recent_since, recent_since),
            ).fetchone()
            if active is not None:
                return None
            connection.execute(
                """INSERT INTO task_results
                   (task_id, origin, request_id, host, runtime, session_id,
                    workspace, state, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'submitted', ?, ?)""",
                (task_id, origin, request_id, host, runtime, session_id,
                 workspace, current, current),
            )
            if request_fingerprint is not None:
                connection.execute(
                    "INSERT INTO request_fingerprints (task_id, fingerprint, created_at) VALUES (?, ?, ?)",
                    (task_id, request_fingerprint, current),
                )
            row = connection.execute(
                "SELECT * FROM task_results WHERE task_id = ?", (task_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def resolve(
        self, task_id: str, *, reason: str | None = None, now: float | None = None,
    ) -> dict[str, Any] | None:
        """Mark one unfinished task resolved by explicit user action.

        This is a gateway disposition, not an execution result: the remote
        session is not cancelled and no completion is fabricated.
        """
        current = time.time() if now is None else now
        note = f"gateway_resolved: {reason}" if reason else "gateway_resolved"
        with _conn(self.path) as connection:
            changed = connection.execute(
                """UPDATE task_results
                   SET state='resolved', outcome='resolved', error=?,
                       updated_at=?, completed_at=?, notify_error=NULL
                   WHERE task_id=? AND state IN ('submitted', 'running', 'uncertain')""",
                (note[:500], current, current, task_id),
            ).rowcount
        return self.get(task_id) if changed else None

    def unfinished_tasks(self, limit: int = 200) -> list[dict[str, Any]]:
        with _conn(self.path) as connection:
            rows = connection.execute(
                """SELECT * FROM task_results
                   WHERE state IN ('submitted', 'running', 'uncertain')
                   ORDER BY updated_at DESC LIMIT ?""",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def open_tasks_for_target(
        self, host: str, runtime: str, session_id: str,
    ) -> list[dict[str, Any]]:
        """Return every unfinished task for one target, newest first."""
        with _conn(self.path) as connection:
            rows = connection.execute(
                """SELECT * FROM task_results
                   WHERE host=? AND runtime=? AND session_id=?
                     AND state IN ('submitted', 'running', 'uncertain')
                   ORDER BY created_at DESC""",
                (host, runtime, session_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def open_task_for_target(
        self, host: str, runtime: str, session_id: str,
    ) -> dict[str, Any] | None:
        """Return the newest unfinished task for one target, if any.

        ``uncertain`` counts as unfinished: the target may already have executed
        the task, so a new dispatch must first be checked by the caller.
        """
        with _conn(self.path) as connection:
            row = connection.execute(
                """SELECT * FROM task_results
                   WHERE host=? AND runtime=? AND session_id=?
                     AND state IN ('submitted', 'running', 'uncertain')
                   ORDER BY created_at DESC LIMIT 1""",
                (host, runtime, session_id),
            ).fetchone()
        return dict(row) if row is not None else None

    def mark_notified(self, task_id: str, error: str | None = None) -> None:
        with _conn(self.path) as connection:
            connection.execute(
                "UPDATE task_results SET notified_at=?, notify_error=? WHERE task_id=?",
                (time.time(), error, task_id),
            )
