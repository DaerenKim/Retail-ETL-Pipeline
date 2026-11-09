"""Warehouse writes: dimensions, the processed-file registry, and the idempotent partition load.

Idempotency in one sentence: a (store_code, business_date) partition is always rewritten as a whole
inside one transaction (delete old rows -> insert new rows -> record which file owns it), so loading
the same data twice, or a corrected v2 file, can never double count.
"""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .config import Settings
from .transform import sql_literal
from .warehouse import Warehouse

FACT_COLUMNS = ["store_code", "business_date", "bill_no", "bill_datetime", "product_code", "quantity",
                "unit_price", "discount", "net_amount", "payment_mode", "is_return", "file_id"]
REJECT_COLUMNS = ["file_id", "run_id", "store_code", "business_date", "row_number", "reason_code",
                  "reason_detail", "net_amount", "raw_record"]
FILE_LOG_COLUMNS = ["run_id", "file_name", "landing_folder", "file_path", "store_code", "business_date",
                    "file_version", "sha256", "bytes", "layout_signature", "column_mapping", "rows_read",
                    "rows_loaded", "rows_rejected", "source_net_amount", "loaded_net_amount", "status",
                    "reason_code", "message", "processed_at"]


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------------- dimensions
def _read_master(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"master file not found: {path}")
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def refresh_dimensions(wh: Warehouse, settings: Settings) -> tuple[int, int]:
    """Upsert data/master/*.csv into dim_store / dim_product. Safe to run every time."""
    now = utcnow()
    stores = _read_master(settings.master_dir / "stores.csv")
    products = _read_master(settings.master_dir / "products.csv")
    with wh.transaction():
        for s in stores:
            wh.execute("""
                INSERT INTO dim_store (store_code, store_name, locality, zone, city, state, format,
                                       pos_system, open_date, sqft, is_active, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_code) DO UPDATE SET
                    store_name = excluded.store_name, locality = excluded.locality, zone = excluded.zone,
                    city = excluded.city, state = excluded.state, format = excluded.format,
                    pos_system = excluded.pos_system, open_date = excluded.open_date, sqft = excluded.sqft,
                    is_active = excluded.is_active, updated_at = excluded.updated_at
            """, (s["store_code"].strip().upper(), s["store_name"], s.get("locality"), s.get("zone"),
                  s.get("city"), s.get("state"), s.get("format"), s.get("pos_system"),
                  s.get("open_date") or None, int(s["sqft"]) if s.get("sqft") else None,
                  (s.get("is_active") or "Y").upper(), now))
        for p in products:
            wh.execute("""
                INSERT INTO dim_product (product_code, product_name, category, brand, pack_size, mrp,
                                         gst_rate, is_active, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (product_code) DO UPDATE SET
                    product_name = excluded.product_name, category = excluded.category,
                    brand = excluded.brand, pack_size = excluded.pack_size, mrp = excluded.mrp,
                    gst_rate = excluded.gst_rate, is_active = excluded.is_active,
                    updated_at = excluded.updated_at
            """, (p["product_code"].strip().upper(), p["product_name"], p.get("category"), p.get("brand"),
                  p.get("pack_size"), float(p["mrp"]), int(p["gst_rate"]) if p.get("gst_rate") else None,
                  (p.get("is_active") or "Y").upper(), now))
    return len(stores), len(products)


def dims_to_duckdb(wh: Warehouse, con: duckdb.DuckDBPyConnection) -> None:
    """Copy the (small) dimension tables into DuckDB so validation can join against them."""
    con.execute("CREATE OR REPLACE TABLE dim_store (store_code VARCHAR PRIMARY KEY, is_active VARCHAR)")
    con.executemany("INSERT INTO dim_store VALUES (?, ?)",
                    wh.query("SELECT store_code, is_active FROM dim_store"))
    con.execute("CREATE OR REPLACE TABLE dim_product (product_code VARCHAR PRIMARY KEY, mrp DECIMAL(12, 2))")
    con.executemany("INSERT INTO dim_product VALUES (?, ?)",
                    [(c, float(m)) for c, m in wh.query("SELECT product_code, mrp FROM dim_product")])


# ---------------------------------------------------------------------------------- file registry
@dataclass
class FileRecord:
    run_id: str
    file_name: str
    landing_folder: str
    file_path: str                      # relative to the landing dir: "<folder>/<name>"
    store_code: str | None
    business_date: str | None
    file_version: int | None
    sha256: str
    bytes: int
    status: str
    reason_code: str | None = None
    message: str | None = None
    layout_signature: str | None = None
    column_mapping: dict | None = None
    rows_read: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    source_net_amount: float | None = None
    loaded_net_amount: float | None = None

    def values(self) -> list:
        return [self.run_id, self.file_name, self.landing_folder, self.file_path, self.store_code,
                self.business_date, self.file_version, self.sha256, self.bytes, self.layout_signature,
                json.dumps(self.column_mapping) if self.column_mapping is not None else None,
                self.rows_read, self.rows_loaded, self.rows_rejected, self.source_net_amount,
                self.loaded_net_amount, self.status, self.reason_code, self.message, utcnow()]


def upsert_file_log(wh: Warehouse, rec: FileRecord) -> int:
    """Insert (or, when a quarantined file is retried, update) the registry row. Returns file_id."""
    cols = ", ".join(FILE_LOG_COLUMNS)
    marks = ", ".join("?" for _ in FILE_LOG_COLUMNS)
    updates = ", ".join(f"{c} = excluded.{c}" for c in FILE_LOG_COLUMNS if c not in ("file_path", "sha256"))
    return wh.insert_returning(
        f"INSERT INTO etl_file_log ({cols}) VALUES ({marks}) "
        f"ON CONFLICT (file_path, sha256) DO UPDATE SET {updates} RETURNING file_id",
        rec.values())


# ---------------------------------------------------------------------------------- partition load
def fetch_valid_rows(con: duckdb.DuckDBPyConnection, business_date: str, file_id: int) -> list[tuple]:
    return con.execute(f"""
        SELECT store_code, {sql_literal(business_date)}, bill_no, strftime(bill_ts, '%Y-%m-%d %H:%M:%S'),
               product_code, CAST(quantity_num AS INTEGER), CAST(unit_price AS DOUBLE),
               CAST(discount AS DOUBLE), CAST(net_amount AS DOUBLE), payment_mode,
               CASE WHEN quantity_num < 0 THEN 1 ELSE 0 END, {int(file_id)}
        FROM validated WHERE reason_code IS NULL ORDER BY row_number
    """).fetchall()


def fetch_reject_rows(con: duckdb.DuckDBPyConnection, file_id: int, run_id: str, store: str,
                      business_date: str, override_reason: str | None = None,
                      override_detail: str | None = None, fill_reason: str | None = None) -> list[tuple]:
    """Rows for etl_rejected_rows.

    override_reason: relabel EVERY row (whole-file quarantine, e.g. unknown store).
    fill_reason:     keep each row's own reason, label otherwise-valid rows with this code.
    """
    if override_reason:
        reason, detail, where = sql_literal(override_reason), sql_literal(override_detail or ""), ""
    elif fill_reason:
        reason = f"COALESCE(reason_code, {sql_literal(fill_reason)})"
        detail = f"COALESCE(reason_detail, {sql_literal('file quarantined: ' + (override_detail or fill_reason))})"
        where = ""
    else:
        reason, detail, where = "reason_code", "reason_detail", "WHERE reason_code IS NOT NULL"
    return con.execute(f"""
        SELECT {int(file_id)}, {sql_literal(run_id)}, {sql_literal(store)}, {sql_literal(business_date)},
               row_number, {reason}, {detail}, CAST(net_amount AS DOUBLE), raw_record
        FROM validated {where} ORDER BY row_number
    """).fetchall()


def current_partition(wh: Warehouse, store: str, business_date: str) -> tuple | None:
    rows = wh.query("SELECT file_id, file_version, sha256 FROM etl_partition "
                    "WHERE store_code = ? AND business_date = ?", (store, business_date))
    return rows[0] if rows else None


def load_partition(wh: Warehouse, con: duckdb.DuckDBPyConnection, rec: FileRecord) -> tuple[int, int | None]:
    """Replace the (store, business_date) partition with this file's rows - atomically.

    Returns (file_id, previous_file_id or None).
    """
    store, bdate = rec.store_code, rec.business_date
    assert store and bdate
    with wh.transaction():
        prev = current_partition(wh, store, bdate)
        wh.execute("DELETE FROM fact_sales WHERE store_code = ? AND business_date = ?", (store, bdate))
        wh.execute("DELETE FROM etl_rejected_rows WHERE store_code = ? AND business_date = ?", (store, bdate))
        file_id = upsert_file_log(wh, rec)
        wh.insert_many("fact_sales", FACT_COLUMNS, fetch_valid_rows(con, bdate, file_id))
        wh.insert_many("etl_rejected_rows", REJECT_COLUMNS,
                       fetch_reject_rows(con, file_id, rec.run_id, store, bdate))
        wh.execute("""
            INSERT INTO etl_partition (store_code, business_date, file_id, file_version, sha256,
                                       rows_loaded, net_amount, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (store_code, business_date) DO UPDATE SET
                file_id = excluded.file_id, file_version = excluded.file_version, sha256 = excluded.sha256,
                rows_loaded = excluded.rows_loaded, net_amount = excluded.net_amount,
                loaded_at = excluded.loaded_at
        """, (store, bdate, file_id, rec.file_version, rec.sha256, rec.rows_loaded,
              rec.loaded_net_amount or 0.0, utcnow()))
        prev_id = None
        if prev and prev[0] != file_id:
            prev_id = prev[0]
            wh.execute("UPDATE etl_file_log SET status = 'SUPERSEDED', "
                       "message = 'replaced by file_id ' || ? WHERE file_id = ?", (str(file_id), prev_id))
    return file_id, prev_id


def quarantine_file(wh: Warehouse, con: duckdb.DuckDBPyConnection, rec: FileRecord,
                    override_reason: str | None = None, override_detail: str | None = None,
                    fill_reason: str | None = None) -> int:
    """Record a quarantined file and ALL its rows as rejects. The warehouse partition is NOT touched."""
    assert rec.store_code and rec.business_date
    with wh.transaction():
        file_id = upsert_file_log(wh, rec)
        wh.execute("DELETE FROM etl_rejected_rows WHERE file_id = ?", (file_id,))
        wh.insert_many("etl_rejected_rows", REJECT_COLUMNS,
                       fetch_reject_rows(con, file_id, rec.run_id, rec.store_code, rec.business_date,
                                         override_reason, override_detail, fill_reason))
    return file_id


# ---------------------------------------------------------------------------------- silver layer
def write_silver(con: duckdb.DuckDBPyConnection, settings: Settings, rec: FileRecord) -> Path:
    """Write the partition's clean rows to Parquet (hive layout). Overwrites -> idempotent too."""
    out_dir = settings.silver_dir / "sales" / f"store_code={rec.store_code}" / f"business_date={rec.business_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    con.execute(f"""
        COPY (
            SELECT bill_no, bill_ts AS bill_datetime, product_code,
                   CAST(quantity_num AS INTEGER) AS quantity,
                   CAST(unit_price AS DECIMAL(12, 2)) AS unit_price,
                   CAST(discount AS DECIMAL(12, 2)) AS discount,
                   CAST(net_amount AS DECIMAL(14, 2)) AS net_amount,
                   payment_mode, quantity_num < 0 AS is_return,
                   {sql_literal(rec.file_name)} AS source_file
            FROM validated WHERE reason_code IS NULL ORDER BY row_number
        ) TO {sql_literal(str(out))} (FORMAT parquet, COMPRESSION zstd)
    """)
    return out
