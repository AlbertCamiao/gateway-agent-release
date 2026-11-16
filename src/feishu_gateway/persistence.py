"""Shared SQLite connection policy and schema-version guard."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

SCHEMA_VERSION = 4


def connect(path: Path | str, *, timeout: float = 5.0) -> sqlite3.Connection:
    db = sqlite3.connect(
        Path(path).resolve().as_uri() + "?mode=rw", uri=True, timeout=timeout,
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA busy_timeout = 5000")
    db.execute("PRAGMA foreign_keys = ON")
    db.execute("PRAGMA synchronous = NORMAL")
    return db


def schema_version(db: sqlite3.Connection) -> int:
    row = db.execute("PRAGMA user_version").fetchone()
    return int(row[0]) if row is not None else 0


def initialize_schema(
    db: sqlite3.Connection, path: Path | str, *,
    version: int = SCHEMA_VERSION, backup: bool = True,
    schema: str | None = None,
) -> int:
    current = schema_version(db)
    if current > version:
        raise RuntimeError(
            f"database schema v{current} is newer than supported v{version}"
        )
    has_tables = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1"
    ).fetchone() is not None
    if db.in_transaction:
        db.commit()
    if current < version and backup and has_tables:
        destination = Path(path).with_name(
            f"{Path(path).name}.bak.v{current}.{int(time.time())}"
        )
        target = sqlite3.connect(destination)
        try:
            db.backup(target)
            target.commit()
        finally:
            target.close()
        os.chmod(destination, 0o600)
    statements = [
        statement.strip() for statement in (schema or "").split(";") if statement.strip()
    ]
    if current == version and not statements:
        return current
    # Apply additive DDL and the version bump in one transaction so a failed
    # migration cannot leave a newer version with partially created tables.
    db.execute("BEGIN IMMEDIATE")
    try:
        for statement in statements:
            db.execute(statement)
        if current < version:
            db.execute(f"PRAGMA user_version = {version}")
        db.commit()
    except Exception:
        db.rollback()
        raise
    return version
