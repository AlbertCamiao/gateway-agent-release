"""Durable conversation-scoped focus for the daemon AskPlanner."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Mapping

from feishu_gateway.persistence import SCHEMA_VERSION, connect, initialize_schema

_SCHEMA = """
CREATE TABLE IF NOT EXISTS conversation_focus (
    origin TEXT NOT NULL,
    chat_id TEXT NOT NULL DEFAULT '',
    sender_id TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL,
    runtime TEXT NOT NULL,
    workspace TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL,
    title TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (origin, chat_id, sender_id)
)
"""


def _conn(path: Path) -> Any:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    connection = connect(path)
    initialize_schema(connection, path, version=SCHEMA_VERSION, schema=_SCHEMA)
    return connection


class FocusStore:
    """Last successfully selected target per conversation.

    Only the target facts are stored: host/runtime/workspace/session/title.
    The original text and model prompt are never persisted here.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        with _conn(self.path):
            pass
        os.chmod(self.path, 0o600)

    @staticmethod
    def _key(origin: str, delivery: Mapping[str, str] | None) -> tuple[str, str, str]:
        return (
            str(origin),
            str((delivery or {}).get("chat_id") or ""),
            str((delivery or {}).get("sender_id") or ""),
        )

    def get(self, origin: str, delivery: Mapping[str, str] | None = None) -> dict[str, str] | None:
        with _conn(self.path) as db:
            row = db.execute(
                """SELECT host, runtime, workspace, session_id, title
                   FROM conversation_focus
                   WHERE origin=? AND chat_id=? AND sender_id=?""",
                self._key(origin, delivery),
            ).fetchone()
        if row is None:
            return None
        return {
            "host": str(row["host"]),
            "runtime": str(row["runtime"]),
            "workspace": str(row["workspace"] or ""),
            "session": str(row["session_id"]),
            **({"title": str(row["title"])} if row["title"] else {}),
        }

    def set(
        self, origin: str, delivery: Mapping[str, str] | None, focus: Mapping[str, str],
    ) -> None:
        host = focus.get("host")
        runtime = focus.get("runtime")
        session = focus.get("session")
        if not all(isinstance(value, str) and value for value in (host, runtime, session)):
            return
        key = self._key(origin, delivery)
        with _conn(self.path) as db, db:
            db.execute(
                """INSERT INTO conversation_focus
                   (origin, chat_id, sender_id, host, runtime, workspace, session_id, title, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(origin, chat_id, sender_id) DO UPDATE SET
                     host=excluded.host, runtime=excluded.runtime,
                     workspace=excluded.workspace, session_id=excluded.session_id,
                     title=excluded.title, updated_at=excluded.updated_at""",
                (
                    key[0], key[1], key[2], host, runtime,
                    str(focus.get("workspace") or ""), session,
                    str(focus["title"]) if focus.get("title") else None,
                    time.time(),
                ),
            )
