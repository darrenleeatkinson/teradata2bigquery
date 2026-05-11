import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class Status(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ScriptRecord:
    path: str
    script_type: str = ""
    status: Status = Status.PENDING
    attempts: int = 0
    error: str = ""
    output_path: str = ""
    translated_at: datetime | None = None


class StateStore:
    def __init__(self, db_path: Path):
        self._conn = sqlite3.connect(str(db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS scripts (
                path         TEXT PRIMARY KEY,
                script_type  TEXT DEFAULT '',
                status       TEXT DEFAULT 'pending',
                attempts     INTEGER DEFAULT 0,
                error        TEXT DEFAULT '',
                output_path  TEXT DEFAULT '',
                translated_at TEXT
            )
        """)
        self._conn.commit()

    def upsert(self, record: ScriptRecord) -> None:
        ts = record.translated_at.isoformat() if record.translated_at else None
        self._conn.execute("""
            INSERT INTO scripts
                (path, script_type, status, attempts, error, output_path, translated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                script_type   = excluded.script_type,
                status        = excluded.status,
                attempts      = excluded.attempts,
                error         = excluded.error,
                output_path   = excluded.output_path,
                translated_at = excluded.translated_at
        """, (record.path, record.script_type, record.status.value,
              record.attempts, record.error, record.output_path, ts))
        self._conn.commit()

    def get(self, path: str) -> ScriptRecord | None:
        row = self._conn.execute(
            "SELECT * FROM scripts WHERE path = ?", (path,)
        ).fetchone()
        return self._row_to_record(row) if row else None

    def all(self) -> list[ScriptRecord]:
        rows = self._conn.execute("SELECT * FROM scripts").fetchall()
        return [self._row_to_record(r) for r in rows]

    def pending(self) -> list[ScriptRecord]:
        rows = self._conn.execute(
            "SELECT * FROM scripts WHERE status IN ('pending', 'in_progress')"
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _row_to_record(row: tuple) -> ScriptRecord:
        path, script_type, status, attempts, error, output_path, ts = row
        return ScriptRecord(
            path=path,
            script_type=script_type,
            status=Status(status),
            attempts=attempts,
            error=error,
            output_path=output_path,
            translated_at=datetime.fromisoformat(ts) if ts else None,
        )
