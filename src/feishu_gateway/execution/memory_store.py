"""Durable LLM-curated conversation memory for the daemon planner.

The store keeps two layers:

* ``memory_events`` is append-only evidence.  Every completed task gets one
  LLM-written event summary and a source task id.
* ``session_profiles`` is the current rolling profile for one conversation
  target.  It is updated from the previous profile plus the new event by the
  curator, never by extracting keywords from the user's text.

Focus (the last selected target) is intentionally a separate store.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from feishu_gateway.persistence import SCHEMA_VERSION, connect, initialize_schema

_SCHEMA = """
CREATE TABLE IF NOT EXISTS memory_events (
    id TEXT PRIMARY KEY,
    origin TEXT NOT NULL,
    chat_id TEXT NOT NULL DEFAULT '',
    sender_id TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL,
    runtime TEXT NOT NULL,
    session_id TEXT NOT NULL,
    title TEXT,
    event_summary TEXT NOT NULL,
    task_excerpt TEXT,
    outcome_excerpt TEXT,
    state TEXT NOT NULL,
    next_step TEXT,
    source_task_id TEXT,
    model TEXT NOT NULL,
    confidence REAL NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS memory_events_key
ON memory_events (origin, chat_id, sender_id, created_at DESC);

CREATE TABLE IF NOT EXISTS session_profiles (
    origin TEXT NOT NULL,
    chat_id TEXT NOT NULL DEFAULT '',
    sender_id TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL,
    runtime TEXT NOT NULL,
    session_id TEXT NOT NULL,
    title TEXT,
    summary TEXT NOT NULL,
    state TEXT NOT NULL,
    next_step TEXT,
    evidence_event_ids TEXT NOT NULL DEFAULT '[]',
    model TEXT NOT NULL,
    confidence REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (origin, chat_id, sender_id, host, runtime, session_id)
);
CREATE INDEX IF NOT EXISTS session_profiles_key
ON session_profiles (origin, chat_id, sender_id, updated_at DESC);
"""


def _conn(path: Path) -> Any:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    connection = connect(path)
    initialize_schema(connection, path, version=SCHEMA_VERSION, schema=_SCHEMA)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(memory_events)")}
    if "task_excerpt" not in columns:
        connection.execute("ALTER TABLE memory_events ADD COLUMN task_excerpt TEXT")
    if "outcome_excerpt" not in columns:
        connection.execute("ALTER TABLE memory_events ADD COLUMN outcome_excerpt TEXT")
    connection.commit()
    return connection


def _key(origin: str, delivery: Mapping[str, str] | None) -> tuple[str, str, str]:
    return (
        str(origin),
        str((delivery or {}).get("chat_id") or ""),
        str((delivery or {}).get("sender_id") or ""),
    )


class MemoryStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        with _conn(self.path):
            pass
        os.chmod(self.path, 0o600)
        self._migrate_legacy_profiles()

    def _migrate_legacy_profiles(self) -> None:
        """Copy pre-append-only summaries once, without deleting the old table."""
        try:
            with _conn(self.path) as db, db:
                db.execute(
                    """INSERT OR IGNORE INTO session_profiles
                       (origin, chat_id, sender_id, host, runtime, session_id, title,
                        summary, state, next_step, evidence_event_ids, model,
                        confidence, updated_at)
                       SELECT origin, chat_id, sender_id, host, runtime, session_id,
                              title, summary, state, next_step, '[]', model,
                              confidence, updated_at
                       FROM conversation_memory"""
                )
        except sqlite3.OperationalError:
            pass

    def recent(
        self, origin: str, delivery: Mapping[str, str] | None = None, limit: int = 20,
    ) -> list[dict[str, str]]:
        key = _key(origin, delivery)
        with _conn(self.path) as db:
            rows = db.execute(
                """SELECT host, runtime, session_id, title, summary, state,
                          next_step, evidence_event_ids
                   FROM session_profiles
                   WHERE origin=? AND chat_id=? AND sender_id=?
                   ORDER BY updated_at DESC LIMIT ?""",
                (*key, max(1, min(int(limit), 50))),
            ).fetchall()
        return [
            {
                "host": str(row["host"]),
                "runtime": str(row["runtime"]),
                "session": str(row["session_id"]),
                "title": str(row["title"]) if row["title"] else "",
                "summary": str(row["summary"]),
                "state": str(row["state"]),
                "next_step": str(row["next_step"]) if row["next_step"] else "",
                "evidence_count": len(json.loads(row["evidence_event_ids"] or "[]")),
            }
            for row in rows
        ]

    def profile(
        self, origin: str, delivery: Mapping[str, str] | None,
        *, host: str, runtime: str, session: str,
    ) -> dict[str, Any] | None:
        key = _key(origin, delivery)
        with _conn(self.path) as db:
            row = db.execute(
                """SELECT * FROM session_profiles
                   WHERE origin=? AND chat_id=? AND sender_id=?
                     AND host=? AND runtime=? AND session_id=?""",
                (*key, host, runtime, session),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["evidence_event_ids"] = list(
            json.loads(result.get("evidence_event_ids") or "[]")
        )
        return result

    def append_event(
        self, origin: str, delivery: Mapping[str, str] | None,
        *, host: str, runtime: str, session: str, title: str | None,
        event_summary: str, state: str, next_step: str | None,
        source_task_id: str | None, model: str, confidence: float,
        task_excerpt: str | None = None, outcome_excerpt: str | None = None,
    ) -> str:
        if not isinstance(event_summary, str) or not event_summary.strip():
            raise ValueError("memory event requires a summary")
        event_id = uuid4().hex
        key = _key(origin, delivery)
        with _conn(self.path) as db, db:
            db.execute(
                """INSERT INTO memory_events
                   (id, origin, chat_id, sender_id, host, runtime, session_id, title,
                    event_summary, task_excerpt, outcome_excerpt, state, next_step,
                    source_task_id, model, confidence, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_id, *key, host, runtime, session, title or None,
                    event_summary.strip(),
                    (task_excerpt or "")[:4000] or None,
                    (outcome_excerpt or "")[:4000] or None,
                    state or "unknown", next_step or None, source_task_id or None,
                    model, max(0.0, min(float(confidence), 1.0)), time.time(),
                ),
            )
        return event_id

    def recent_events(
        self, origin: str, delivery: Mapping[str, str] | None = None, limit: int = 30,
    ) -> list[dict[str, Any]]:
        key = _key(origin, delivery)
        with _conn(self.path) as db:
            rows = db.execute(
                """SELECT id, host, runtime, session_id, title, event_summary,
                          task_excerpt, outcome_excerpt, state, source_task_id,
                          created_at
                   FROM memory_events
                   WHERE origin=? AND chat_id=? AND sender_id=?
                   ORDER BY created_at DESC LIMIT ?""",
                (*key, max(1, min(int(limit), 100))),
            ).fetchall()
        return [dict(row) for row in rows]

    def evidence(self, event_ids: list[str]) -> list[dict[str, Any]]:
        if not event_ids:
            return []
        placeholders = ",".join("?" for _ in event_ids)
        with _conn(self.path) as db:
            rows = db.execute(
                f"""SELECT id, host, runtime, session_id, title, event_summary,
                           task_excerpt, outcome_excerpt, state, source_task_id,
                           created_at
                    FROM memory_events WHERE id IN ({placeholders})""",
                tuple(event_ids),
            ).fetchall()
        by_id = {str(row["id"]): dict(row) for row in rows}
        return [by_id[event_id] for event_id in event_ids if event_id in by_id]

    def upsert_profile(
        self, origin: str, delivery: Mapping[str, str] | None,
        *, host: str, runtime: str, session: str, title: str | None,
        summary: str, state: str, next_step: str | None,
        evidence_event_ids: list[str], model: str, confidence: float,
    ) -> None:
        if not all(isinstance(value, str) and value for value in (host, runtime, session, summary)):
            raise ValueError("memory profile requires target and summary")
        key = _key(origin, delivery)
        with _conn(self.path) as db, db:
            db.execute(
                """INSERT INTO session_profiles
                   (origin, chat_id, sender_id, host, runtime, session_id, title,
                    summary, state, next_step, evidence_event_ids, model,
                    confidence, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(origin, chat_id, sender_id, host, runtime, session_id)
                   DO UPDATE SET
                     title=excluded.title, summary=excluded.summary,
                     state=excluded.state, next_step=excluded.next_step,
                     evidence_event_ids=excluded.evidence_event_ids,
                     model=excluded.model, confidence=excluded.confidence,
                     updated_at=excluded.updated_at""",
                (
                    *key, host, runtime, session, title or None,
                    summary, state or "unknown", next_step or None,
                    json.dumps(list(evidence_event_ids)), model,
                    max(0.0, min(float(confidence), 1.0)), time.time(),
                ),
            )

    # Compatibility for callers/tests that only need the current profile.
    def upsert(
        self, origin: str, delivery: Mapping[str, str] | None,
        *, host: str, runtime: str, session: str, title: str | None,
        summary: str, state: str, next_step: str | None,
        model: str, confidence: float,
    ) -> None:
        self.upsert_profile(
            origin, delivery, host=host, runtime=runtime, session=session,
            title=title, summary=summary, state=state, next_step=next_step,
            evidence_event_ids=[], model=model, confidence=confidence,
        )


class MemoryCurator:
    """LLM curator: summarize one event, then roll the session profile."""

    _PROPERTIES = {
        "eventSummary": {"type": "string"},
        "profileSummary": {"type": "string"},
        "state": {"type": "string"},
        "nextStep": {"type": "string"},
        "confidence": {"type": "number"},
    }
    _INSTRUCTIONS = (
        "You are the memory curator for a coding-agent gateway. "
        "Given one completed task event and the previous session profile, write "
        "a concise event summary and an updated cumulative profile summary in "
        "the user's language. Use only the supplied task and evidence. Do not "
        "invent results. Preserve useful earlier context in the profile. "
        "Use state=unknown when evidence is insufficient. Return only the "
        "requested JSON fields; never output shell commands."
    )

    def __init__(self, router, store: MemoryStore, *, timeout: float = 20.0) -> None:
        self._router = router
        self._store = store
        self._timeout = timeout

    async def record(
        self, origin: str, delivery: Mapping[str, str] | None, *,
        host: str, runtime: str, session: str, title: str | None,
        task: str, state: str, outcome: str | None, source_task_id: str | None = None,
    ) -> None:
        previous = await asyncio.to_thread(
            self._store.profile, origin, delivery,
            host=host, runtime=runtime, session=session,
        )
        payload = json.dumps({
            "target": {"host": host, "runtime": runtime, "session_id": session, "title": title},
            "task": task,
            "state": state,
            "outcome": outcome,
            "previousProfile": previous,
        }, ensure_ascii=False).encode()
        try:
            result = await self._router.complete(
                self._INSTRUCTIONS, payload, self._PROPERTIES, timeout=self._timeout,
            )
        except Exception:
            return
        event_summary = result.get("eventSummary")
        profile_summary = result.get("profileSummary") or event_summary
        if not isinstance(event_summary, str) or not event_summary.strip():
            return
        if not isinstance(profile_summary, str) or not profile_summary.strip():
            profile_summary = event_summary
        confidence = result.get("confidence", 0.0)
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.0
        event_id = await asyncio.to_thread(
            self._store.append_event,
            origin, delivery,
            host=host, runtime=runtime, session=session, title=title,
            event_summary=event_summary, state=str(result.get("state") or "unknown"),
            next_step=str(result.get("nextStep") or "") or None,
            source_task_id=source_task_id, model="gateway-memory",
            confidence=confidence, task_excerpt=task, outcome_excerpt=outcome,
        )
        evidence = list((previous or {}).get("evidence_event_ids") or ())
        evidence.append(event_id)
        await asyncio.to_thread(
            self._store.upsert_profile,
            origin, delivery,
            host=host, runtime=runtime, session=session, title=title,
            summary=profile_summary.strip(), state=str(result.get("state") or "unknown"),
            next_step=str(result.get("nextStep") or "") or None,
            evidence_event_ids=evidence, model="gateway-memory",
            confidence=confidence,
        )


class MemoryRetriever:
    """LLM selection of relevant past events for the current request.

    The planner receives only selected evidence; unrelated history is not
    injected and cannot crowd out the current request.
    """

    _PROPERTIES = {
        "selectedEventIds": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number"},
    }
    _INSTRUCTIONS = (
        "You select relevant past-work evidence for a coding-agent gateway. "
        "The current request is authoritative. Select only event ids whose "
        "summaries or task excerpts are genuinely needed to resolve the current "
        "request, for example when the user explicitly refers to a previous "
        "session or previous work. Return an empty list for a normal follow-up "
        "that should stay on the current target. Never use keyword matching; "
        "use semantic reasoning. Return only the requested JSON fields."
    )

    def __init__(self, router, *, timeout: float = 15.0) -> None:
        self._router = router
        self._timeout = timeout

    async def select(
        self, request: str, focus: Mapping[str, Any] | None,
        profiles: list[Mapping[str, Any]], events: list[Mapping[str, Any]],
    ) -> list[str]:
        if not events:
            return []
        compact_profiles = [
            {
                "host": item.get("host"), "runtime": item.get("runtime"),
                "session": item.get("session"), "title": item.get("title"),
                "summary": item.get("summary"),
            }
            for item in profiles
        ]
        compact_events = [
            {
                "id": item.get("id"), "host": item.get("host"),
                "runtime": item.get("runtime"), "session": item.get("session_id"),
                "title": item.get("title"), "summary": item.get("event_summary"),
            }
            for item in events
        ]
        payload = json.dumps({
            "current_request": request,
            "focus": dict(focus) if focus else None,
            "profiles": compact_profiles,
            "events": compact_events,
        }, ensure_ascii=False).encode()
        try:
            result = await self._router.complete(
                self._INSTRUCTIONS, payload, self._PROPERTIES,
                timeout=self._timeout,
            )
        except Exception:
            return []
        selected = result.get("selectedEventIds")
        if not isinstance(selected, list):
            return []
        known = {str(item.get("id")) for item in events}
        return [item for item in selected if isinstance(item, str) and item in known][:5]
