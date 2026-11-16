"""Per-task delivery identity and a durable Feishu egress outbox."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping

from feishu_gateway.persistence import connect as sqlite_connect, initialize_schema


_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_deliveries (
    task_id TEXT PRIMARY KEY NOT NULL,
    origin TEXT NOT NULL,
    chat_id TEXT,
    sender_id TEXT,
    message_id TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS egress_outbox (
    task_id TEXT PRIMARY KEY NOT NULL,
    chat_id TEXT NOT NULL,
    text TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS egress_outbox_pending
ON egress_outbox (state, next_attempt_at);
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
);
"""


def _conn(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    connection = sqlite_connect(path)
    initialize_schema(connection, path, schema=_SCHEMA)
    return connection


class DeliveryStore:
    MAX_TEXT = 16384
    MAX_ATTEMPTS = 8

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        with _conn(self.path):
            pass
        os.chmod(self.path, 0o600)

    def record(
        self, task_id: str, origin: str,
        delivery: Mapping[str, str] | None = None, *, now: float | None = None,
    ) -> None:
        if not isinstance(task_id, str) or not task_id or len(task_id.encode()) > 256:
            raise ValueError("invalid task id")
        if not isinstance(origin, str) or not origin:
            raise ValueError("invalid origin")
        chat_id = delivery.get("chat_id") if delivery else None
        sender_id = delivery.get("sender_id") if delivery else None
        message_id = delivery.get("message_id") if delivery else None
        with _conn(self.path) as db, db:
            db.execute(
                """INSERT INTO task_deliveries
                   (task_id, origin, chat_id, sender_id, message_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(task_id) DO NOTHING""",
                (task_id, origin, chat_id, sender_id, message_id,
                 time.time() if now is None else now),
            )

    def delivery(self, task_id: str) -> dict[str, Any] | None:
        with _conn(self.path) as db:
            row = db.execute(
                "SELECT * FROM task_deliveries WHERE task_id=?", (task_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def enqueue(
        self, task_id: str, chat_id: str, text: str, *, now: float | None = None,
    ) -> None:
        if not isinstance(task_id, str) or not task_id or len(task_id.encode()) > 256:
            raise ValueError("invalid task id")
        if not isinstance(chat_id, str) or not chat_id or not chat_id.isprintable():
            raise ValueError("invalid delivery chat")
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > self.MAX_TEXT:
            raise ValueError("invalid delivery text")
        current = time.time() if now is None else now
        with _conn(self.path) as db, db:
            db.execute(
                """INSERT INTO egress_outbox
                   (task_id, chat_id, text, state, next_attempt_at, created_at, updated_at)
                   VALUES (?, ?, ?, 'pending', ?, ?, ?) ON CONFLICT(task_id) DO NOTHING""",
                (task_id, chat_id, text, current, current, current),
            )

    def outbox(self, task_id: str) -> dict[str, Any] | None:
        with _conn(self.path) as db:
            row = db.execute(
                "SELECT * FROM egress_outbox WHERE task_id=?", (task_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def claim(self, task_id: str, *, now: float | None = None) -> bool:
        """Atomically take a pending row for one sender.

        Two completion paths (completion server and transcript watcher) may
        race for the same task. Only the winner may send; the loser returns
        without a second Feishu message.
        """
        current = time.time() if now is None else now
        with _conn(self.path) as db, db:
            cursor = db.execute(
                """UPDATE egress_outbox SET state='sending', updated_at=?
                   WHERE task_id=? AND state='pending'""",
                (current, task_id),
            )
        return cursor.rowcount == 1

    def reclaim_stale(self, *, now: float | None = None, older_than: float = 120.0) -> None:
        current = time.time() if now is None else now
        with _conn(self.path) as db, db:
            db.execute(
                """UPDATE egress_outbox
                   SET state='pending', next_attempt_at=?, updated_at=?
                   WHERE state='sending' AND updated_at <= ?""",
                (current, current, current - older_than),
            )

    def pending(self, now: float | None = None, limit: int = 8) -> list[dict[str, Any]]:
        current = time.time() if now is None else now
        self.reclaim_stale(now=current)
        with _conn(self.path) as db:
            rows = db.execute(
                """SELECT * FROM egress_outbox
                   WHERE state='pending' AND next_attempt_at <= ?
                   ORDER BY next_attempt_at LIMIT ?""",
                (current, max(1, min(int(limit), 32))),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_sent(self, task_id: str, *, now: float | None = None) -> None:
        current = time.time() if now is None else now
        with _conn(self.path) as db, db:
            db.execute(
                """UPDATE egress_outbox SET state='sent', updated_at=?, last_error=NULL
                   WHERE task_id=?""",
                (current, task_id),
            )

    def fail(
        self, task_id: str, error: str, *, now: float | None = None,
        max_attempts: int | None = None,
    ) -> int:
        current = time.time() if now is None else now
        limit = self.MAX_ATTEMPTS if max_attempts is None else int(max_attempts)
        with _conn(self.path) as db, db:
            row = db.execute(
                "SELECT attempts FROM egress_outbox WHERE task_id=?", (task_id,),
            ).fetchone()
            attempts = (int(row["attempts"]) if row is not None else 0) + 1
            state = "failed" if attempts >= limit else "pending"
            delay = min(300.0, 5.0 * (2 ** max(0, attempts - 1)))
            db.execute(
                """UPDATE egress_outbox
                   SET attempts=?, state=?, next_attempt_at=?, last_error=?, updated_at=?
                   WHERE task_id=? AND state IN ('pending', 'sending')""",
                (attempts, state, current + delay, str(error)[:500], current, task_id),
            )
        return attempts
