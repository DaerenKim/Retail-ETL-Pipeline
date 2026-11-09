"""Read a landing CSV into DuckDB as raw text.

The header is read with Python's csv module (handles the UTF-8 BOM and quoting), then DuckDB
reads the body with every column as VARCHAR under positional names c0..cN. Nothing is typed or
cleaned here, so nothing can be silently lost: typing happens in transform.py where failures
become NULLs that validation turns into reason codes.
"""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from .transform import sql_literal


class EmptyFileError(Exception):
    """Zero-byte file, or no header line."""


class UnreadableFileError(Exception):
    """The CSV could not be parsed (bad encoding, ragged rows...)."""


def read_header(path: Path) -> list[str]:
    try:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            header = next(csv.reader(fh), None)
    except UnicodeDecodeError as exc:
        raise UnreadableFileError(f"not valid UTF-8: {exc}") from exc
    if not header or all(not h.strip() for h in header):
        raise EmptyFileError("file has no header row")
    return header


def load_raw(con: duckdb.DuckDBPyConnection, path: Path, n_columns: int, table: str = "raw") -> int:
    """Create TEMP table `table` (row_number, c0..cN as VARCHAR). Returns the number of data rows.

    row_number is the 1-based data row (header excluded), so rejects can point back to the file.
    """
    columns = "{" + ", ".join(f"'c{i}': 'VARCHAR'" for i in range(n_columns)) + "}"
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {table} AS
            SELECT row_number() OVER () AS row_number, *
            FROM read_csv({sql_literal(str(path))},
                          header = true, auto_detect = false, columns = {columns},
                          delim = ',', quote = '"', escape = '"',
                          strict_mode = true, null_padding = false)
        """)
    except duckdb.Error as exc:
        raise UnreadableFileError(str(exc).splitlines()[0]) from exc
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
