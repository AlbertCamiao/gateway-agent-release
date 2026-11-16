"""Persist Feishu message claims and execution acknowledgements across restarts."""

import os
import re
import sqlite3
import stat
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

from feishu_gateway.persistence import connect as sqlite_connect, initialize_schema


_TRANSITIONS = {
    "claimed": {"queried", "preview_submitting", "preview_ready", "failed"},
    "preview_submitting": {"preview_started", "failed", "uncertain"},
    "preview_started": {"preview_ready", "failed", "uncertain"},
    "preview_ready": {"preview_delivered", "failed", "uncertain"},
    "preview_delivered": {"execution_submitting", "failed", "preview_only"},
    "execution_submitting": {"execution_queued", "execution_started", "failed", "uncertain"},
    "execution_queued": {"execution_started", "failed", "uncertain"},
    "execution_started": {"awaiting_approval", "completed", "failed", "uncertain"},
    "awaiting_approval": {"completed", "failed", "uncertain"},
    "uncertain": {"completed", "failed"},
}
_STAGE_FIELDS = {
    "preview_submitting": {"host", "thread_id"},
    "preview_started": {"preview_turn_id"},
    "preview_ready": {"preview_sha256"},
    "execution_started": {"execution_turn_id"},
}
_SHA256 = re.compile(r"[a-f0-9]{64}")
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS execution_journal (
    message_id TEXT PRIMARY KEY NOT NULL,
    stage TEXT NOT NULL,
    host TEXT,
    thread_id TEXT,
    preview_request_id TEXT NOT NULL,
    execution_request_id TEXT NOT NULL,
    preview_turn_id TEXT,
    preview_sha256 TEXT,
    execution_turn_id TEXT,
    delivery_chat_id TEXT,
    prompt TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_SCOPE = """
CREATE TABLE IF NOT EXISTS journal_scope (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    activated_at_ms INTEGER NOT NULL,
    scope_digest TEXT
)
"""
_CREATE_HISTORY = """
CREATE TABLE IF NOT EXISTS execution_journal_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    host TEXT,
    thread_id TEXT,
    runtime TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_HISTORY_INDEX = """
CREATE INDEX IF NOT EXISTS execution_journal_history_message
ON execution_journal_history (message_id, id)
"""
_CREATE_PROJECTS = """
CREATE TABLE IF NOT EXISTS execution_target_projects (
    message_id TEXT PRIMARY KEY NOT NULL REFERENCES execution_journal(message_id),
    project_root TEXT NOT NULL,
    runtime TEXT NOT NULL DEFAULT 'codex'
)
"""
_CREATE_COMPLETION_INBOX = """
CREATE TABLE IF NOT EXISTS completion_inbox (
    event_id TEXT PRIMARY KEY NOT NULL,
    source TEXT NOT NULL,
    host TEXT,
    runtime TEXT,
    session_id TEXT,
    turn_id TEXT,
    task_id TEXT,
    state TEXT NOT NULL,
    output TEXT,
    seq INTEGER,
    completion_seq INTEGER,
    created_at REAL NOT NULL,
    consumed_at REAL,
    UNIQUE(source, session_id, turn_id)
)
"""
_SELECT = """
SELECT j.*, p.project_root, COALESCE(p.runtime, 'codex') AS runtime
FROM execution_journal AS j
LEFT JOIN execution_target_projects AS p USING (message_id)
"""


class JournalConflict(RuntimeError):
    """The requested change is incompatible with the persisted phase."""


@dataclass(frozen=True, slots=True)
class JournalEntry:
    message_id: str
    stage: str
    host: str | None
    thread_id: str | None
    preview_request_id: str
    execution_request_id: str
    preview_turn_id: str | None
    preview_sha256: str | None
    execution_turn_id: str | None
    created_at: str
    updated_at: str
    project_root: str | None = None
    runtime: str = "codex"
    delivery_chat_id: str | None = None
    prompt: str | None = None


def _valid_identifier(value: str) -> bool:
    return (
        isinstance(value, str) and 0 < len(value) <= 256
        and value.isascii() and value.isprintable() and " " not in value
    )


def _select_entry(db: sqlite3.Connection, message_id: str) -> JournalEntry | None:
    row = db.execute(
        _SELECT + " WHERE j.message_id = ?", (message_id,)
    ).fetchone()
    return JournalEntry(**dict(row)) if row is not None else None


class ExecutionJournal:
    def __init__(self, path: Path, *, scope_digest: str | None = None) -> None:
        if scope_digest is not None and (
            not isinstance(scope_digest, str) or not _SHA256.fullmatch(scope_digest)
        ):
            raise ValueError("Invalid journal scope digest.")
        path = Path(path)
        parent = path.parent.resolve(strict=True)
        directory = parent.stat()
        if (
            not stat.S_ISDIR(directory.st_mode)
            or directory.st_uid != os.getuid()
            or directory.st_mode & 0o022
        ):
            raise PermissionError("The journal directory must be writable only by its owner.")
        self._path = parent / path.name
        descriptor = os.open(
            self._path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600
        )
        try:
            entry = os.fstat(descriptor)
            if (
                not stat.S_ISREG(entry.st_mode)
                or entry.st_uid != os.getuid()
                or stat.S_IMODE(entry.st_mode) != 0o600
            ):
                raise PermissionError("The journal must be a private file owned by this user.")
        finally:
            os.close(descriptor)
        with closing(self._connect()) as db, db:
            initialize_schema(db, self._path)
            db.execute("BEGIN IMMEDIATE")
            db.execute(_CREATE_TABLE)
            db.execute(_CREATE_SCOPE)
            db.execute(_CREATE_PROJECTS)
            db.execute(_CREATE_COMPLETION_INBOX)
            inbox_columns = {row[1] for row in db.execute("PRAGMA table_info(completion_inbox)")}
            if "attempts" not in inbox_columns:
                db.execute("ALTER TABLE completion_inbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
            if "last_error" not in inbox_columns:
                db.execute("ALTER TABLE completion_inbox ADD COLUMN last_error TEXT")
            if "host" not in inbox_columns:
                db.execute("ALTER TABLE completion_inbox ADD COLUMN host TEXT")
            if "runtime" not in inbox_columns:
                db.execute("ALTER TABLE completion_inbox ADD COLUMN runtime TEXT")
            db.execute(_CREATE_HISTORY)
            db.execute(_CREATE_HISTORY_INDEX)
            if "runtime" not in {column["name"] for column in db.execute(
                "PRAGMA table_info(execution_target_projects)"
            )}:
                db.execute("ALTER TABLE execution_target_projects ADD COLUMN runtime TEXT NOT NULL DEFAULT 'codex'")
            if "delivery_chat_id" not in {column["name"] for column in db.execute(
                "PRAGMA table_info(execution_journal)"
            )}:
                db.execute("ALTER TABLE execution_journal ADD COLUMN delivery_chat_id TEXT")
            if "prompt" not in {column["name"] for column in db.execute(
                "PRAGMA table_info(execution_journal)"
            )}:
                db.execute("ALTER TABLE execution_journal ADD COLUMN prompt TEXT")
            db.execute(
                """INSERT INTO journal_scope (singleton, activated_at_ms, scope_digest)
                   VALUES (1, ?, ?) ON CONFLICT(singleton) DO NOTHING""",
                (time.time_ns() // 1_000_000, scope_digest),
            )
            scope = db.execute(
                "SELECT activated_at_ms, scope_digest FROM journal_scope WHERE singleton = 1"
            ).fetchone()
            if scope is None or scope["scope_digest"] != scope_digest:
                raise JournalConflict("The journal belongs to a different Feishu binding.")
            self.activated_at_ms: int = scope["activated_at_ms"]

    def _connect(self) -> sqlite3.Connection:
        db = sqlite_connect(self._path)
        db.execute("PRAGMA synchronous = FULL")
        return db

    def claim(self, message_id: str) -> JournalEntry | None:
        """Claim a message once, or return None if any process has already claimed it."""
        if not _valid_identifier(message_id):
            raise ValueError("Invalid Feishu message ID.")
        with closing(self._connect()) as db, db:
            inserted = db.execute(
                """INSERT INTO execution_journal
                   (message_id, stage, preview_request_id, execution_request_id)
                   VALUES (?, 'claimed', ?, ?) ON CONFLICT(message_id) DO NOTHING""",
                (message_id, str(uuid4()), str(uuid4())),
            ).rowcount
            return _select_entry(db, message_id) if inserted else None

    def get(self, message_id: str) -> JournalEntry | None:
        with closing(self._connect()) as db:
            return _select_entry(db, message_id)

    def set_prompt(self, message_id: str, prompt: str) -> None:
        if not _valid_identifier(message_id) or not isinstance(prompt, str):
            raise ValueError("Invalid prompt.")
        if not prompt.strip() or len(prompt.encode()) > 65536:
            raise ValueError("Invalid prompt.")
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE execution_journal SET prompt=?, updated_at=CURRENT_TIMESTAMP WHERE message_id=?",
                (prompt, message_id),
            )

    def record_delivery_chat(self, message_id: str, chat_id: str) -> None:
        if not _valid_identifier(message_id) or not isinstance(chat_id, str) or not chat_id.startswith("oc_"):
            raise ValueError("Invalid delivery chat.")
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE execution_journal SET delivery_chat_id=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE message_id=?",
                (chat_id, message_id),
            )

    def delivery_chat(self, message_id: str) -> str | None:
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT delivery_chat_id FROM execution_journal WHERE message_id=?",
                (message_id,),
            ).fetchone()
        return row[0] if row is not None and isinstance(row[0], str) else None

    def history(self, message_id: str) -> tuple[JournalEntry, ...]:
        base = self.get(message_id)
        if base is None:
            return ()
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT stage, host, thread_id, runtime FROM execution_journal_history "
                "WHERE message_id=? ORDER BY id",
                (message_id,),
            ).fetchall()
        return tuple(
            JournalEntry(
                base.message_id, row["stage"], row["host"] or base.host,
                row["thread_id"] or base.thread_id, base.preview_request_id,
                base.execution_request_id, base.preview_turn_id, base.preview_sha256,
                base.execution_turn_id, base.created_at, base.updated_at,
                base.project_root, row["runtime"] or base.runtime,
            )
            for row in rows
        )

    def record_inbox_event(
        self, event_id: str, source: str, state: str,
        *, host: str | None = None, runtime: str | None = None,
        session_id: str | None = None, turn_id: str | None = None,
        task_id: str | None = None, output: str | None = None,
        seq: int | None = None, completion_seq: int | None = None,
    ) -> bool:
        if not _valid_identifier(event_id) or not _valid_identifier(source):
            raise ValueError("Invalid completion event.")
        if state not in {"working", "blocked", "completed", "failed", "uncertain"}:
            raise ValueError("Invalid completion state.")
        if output is not None and (not isinstance(output, str) or len(output.encode()) > 262144):
            raise ValueError("Invalid completion output.")
        with closing(self._connect()) as db, db:
            if seq is not None and session_id is not None:
                last = db.execute(
                    "SELECT MAX(seq) FROM completion_inbox WHERE source=? AND session_id=?",
                    (source, session_id),
                ).fetchone()[0]
                if last is not None and seq <= last:
                    return False
            inserted = db.execute(
                """INSERT INTO completion_inbox
                   (event_id, source, host, runtime, session_id, turn_id, task_id, state, output, seq, completion_seq, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT DO NOTHING""",
                (event_id, source, host, runtime, session_id, turn_id, task_id, state, output, seq, completion_seq, time.time()),
            ).rowcount
        return bool(inserted)

    def mark_inbox_attempt(self, event_id: str, error: str) -> int:
        if not _valid_identifier(event_id):
            raise ValueError("Invalid completion event.")
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE completion_inbox SET attempts=attempts+1, last_error=? WHERE event_id=?",
                (error[:500], event_id),
            )
            row = db.execute(
                "SELECT attempts FROM completion_inbox WHERE event_id=?", (event_id,),
            ).fetchone()
        return int(row[0]) if row else 0

    def pending_inbox(self, limit: int = 32) -> tuple[dict, ...]:
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT event_id, source, host, runtime, session_id, turn_id, task_id, state, output, seq, completion_seq "
                "FROM completion_inbox WHERE consumed_at IS NULL ORDER BY created_at LIMIT ?",
                (max(1, min(limit, 128)),),
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def consume_inbox(self, event_id: str) -> None:
        if not _valid_identifier(event_id):
            raise ValueError("Invalid completion event.")
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE completion_inbox SET consumed_at=? WHERE event_id=? AND consumed_at IS NULL",
                (time.time(), event_id),
            )

    def find_by_target_execution_ref(
        self, host: str, runtime: str, session_id: str, execution_ref: str,
    ) -> JournalEntry | None:
        if not all(_valid_identifier(value) for value in (host, runtime, session_id, execution_ref)):
            return None
        with closing(self._connect()) as db:
            rows = db.execute(
                _SELECT + """ WHERE COALESCE(j.host, 'local') = ?
                   AND COALESCE(p.runtime, 'codex') = ?
                   AND j.thread_id = ?
                   AND (j.execution_turn_id = ? OR j.execution_request_id = ?)
                   AND j.stage NOT IN ('completed','failed','rejected','resolved')
                   ORDER BY j.updated_at DESC LIMIT 2""",
                (host, runtime, session_id, execution_ref, execution_ref),
            ).fetchall()
        return JournalEntry(**dict(rows[0])) if len(rows) == 1 else None

    def pending_execution(self) -> tuple[JournalEntry, ...]:
        """Read entries whose execution status should be reconciled after restart."""
        with closing(self._connect()) as db:
            rows = db.execute(
                _SELECT + """ WHERE j.stage IN ('claimed', 'preview_submitting', 'preview_started',
                                   'preview_delivered', 'execution_submitting', 'execution_queued',
                                   'execution_started', 'awaiting_approval')
                   ORDER BY j.created_at, j.message_id"""
            )
            return tuple(JournalEntry(**dict(row)) for row in rows)

    def advance(
        self,
        message_id: str,
        stage: str,
        *,
        host: str | None = None,
        thread_id: str | None = None,
        preview_turn_id: str | None = None,
        preview_sha256: str | None = None,
        execution_turn_id: str | None = None,
        project_root: str | None = None,
        runtime: str = "codex",
    ) -> JournalEntry:
        supplied = {
            field: value
            for field, value in (
                ("host", host), ("thread_id", thread_id), ("preview_turn_id", preview_turn_id),
                ("preview_sha256", preview_sha256), ("execution_turn_id", execution_turn_id),
            )
            if value is not None
        }
        new_preview = stage == "preview_ready" and project_root is not None
        if runtime not in ("codex", "dsh") or (runtime == "dsh" and not new_preview):
            raise ValueError("Invalid runtime for journal phase.")
        expected_fields = (
            {"host", "thread_id", "preview_sha256"}
            if new_preview else _STAGE_FIELDS.get(stage, set())
        )
        if supplied.keys() != expected_fields:
            raise ValueError("Incorrect journal fields for this phase.")
        for field, value in supplied.items():
            if not _valid_identifier(value) or (field == "preview_sha256" and not _SHA256.fullmatch(value)):
                raise ValueError("Invalid journal field value.")
        if project_root is not None:
            if (
                not (stage == "preview_submitting" or new_preview)
                or not isinstance(project_root, str)
                or not project_root.isprintable()
            ):
                raise ValueError("Invalid journal project root.")
            root = PurePosixPath(project_root)
            if not root.is_absolute() or ".." in root.parts or str(root) != project_root:
                raise ValueError("Invalid journal project root.")

        with closing(self._connect()) as db, db:
            # Check and change the phase under one write lock. A second process
            # can never pass the check with a stale copy of the same record.
            db.execute("BEGIN IMMEDIATE")
            entry = _select_entry(db, message_id)
            if entry is None or stage not in _TRANSITIONS.get(entry.stage, ()):
                raise JournalConflict("Invalid execution journal transition.")
            if stage == "preview_ready" and new_preview != (entry.stage == "claimed"):
                raise JournalConflict("A prepared Prompt must be bound to a newly claimed message.")
            if stage == "execution_queued" and entry.runtime != "dsh":
                raise JournalConflict("Only the DSH bridge acknowledges an execution before assigning a turn.")
            if stage == "execution_started" and execution_turn_id == entry.preview_turn_id:
                raise JournalConflict("The execution and preview must be different Codex turns.")
            changes = {"stage": stage, **supplied}
            assignments = ", ".join(f"{field} = ?" for field in changes)
            db.execute(
                f"UPDATE execution_journal SET {assignments}, updated_at = CURRENT_TIMESTAMP "
                "WHERE message_id = ?",
                (*changes.values(), message_id),
            )
            if project_root is not None:
                db.execute(
                    "INSERT INTO execution_target_projects (message_id, project_root, runtime) VALUES (?, ?, ?)",
                    (message_id, project_root, runtime),
                )
            updated = _select_entry(db, message_id)
            assert updated is not None
            db.execute(
                """INSERT INTO execution_journal_history
                   (message_id, stage, host, thread_id, runtime)
                   VALUES (?, ?, ?, ?, ?)""",
                (message_id, updated.stage, updated.host, updated.thread_id, updated.runtime),
            )
            return updated
