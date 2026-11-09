"""Thin database layer so the pipeline runs unchanged on SQLite (default) or PostgreSQL.

SQL in the pipeline is written with `?` placeholders; for PostgreSQL they are rewritten to `%s`.
Bulk inserts use executemany on SQLite and COPY on PostgreSQL.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from .config import PROJECT_ROOT


class Warehouse:
    def __init__(self, url: str):
        self.url = url
        if url.startswith("sqlite:///"):
            self.dialect = "sqlite"
            path = Path(url[len("sqlite:///"):])
            path.parent.mkdir(parents=True, exist_ok=True)
            self.path = path
            self.conn = sqlite3.connect(path, isolation_level=None)   # explicit BEGIN/COMMIT below
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA temp_store = MEMORY")
            self.conn.execute("PRAGMA cache_size = -200000")
        else:
            import psycopg  # imported lazily: only needed for PostgreSQL

            self.dialect = "postgres"
            self.path = None
            self.conn = psycopg.connect(url.replace("postgres://", "postgresql://", 1), autocommit=True)
        self._in_tx = False

    # ------------------------------------------------------------------ basics
    def _sql(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.dialect == "postgres" else sql

    def execute(self, sql: str, params: Sequence = ()) -> None:
        self.conn.execute(self._sql(sql), params)

    def query(self, sql: str, params: Sequence = ()) -> list[tuple]:
        cur = self.conn.execute(self._sql(sql), params)
        return cur.fetchall()

    def scalar(self, sql: str, params: Sequence = ()):
        rows = self.query(sql, params)
        return rows[0][0] if rows else None

    def insert_returning(self, sql: str, params: Sequence) -> int:
        return self.query(sql, params)[0][0]

    def insert_many(self, table: str, columns: list[str], rows: Iterable[Sequence]) -> None:
        cols = ", ".join(columns)
        if self.dialect == "sqlite":
            marks = ", ".join("?" for _ in columns)
            self.conn.executemany(f"INSERT INTO {table} ({cols}) VALUES ({marks})", rows)
        else:
            with self.conn.cursor() as cur:
                with cur.copy(f"COPY {table} ({cols}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(row)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """All-or-nothing block. Any exception rolls everything back."""
        if self.dialect == "sqlite":
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")
        else:
            with self.conn.transaction():
                yield

    def run_script(self, path: Path) -> None:
        sql = path.read_text()
        if self.dialect == "sqlite":
            self.conn.executescript(sql)
        else:
            self.conn.execute(sql)

    def init_schema(self) -> Path:
        script = PROJECT_ROOT / "sql" / f"schema_{'sqlite' if self.dialect == 'sqlite' else 'postgres'}.sql"
        self.run_script(script)
        return script

    def tables(self) -> set[str]:
        if self.dialect == "sqlite":
            rows = self.query("SELECT name FROM sqlite_master WHERE type = 'table'")
        else:
            rows = self.query("SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()")
        return {r[0] for r in rows}

    def close(self) -> None:
        self.conn.close()
