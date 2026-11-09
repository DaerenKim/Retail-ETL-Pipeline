from __future__ import annotations

import csv
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

from etl.config import PROJECT_ROOT, load_settings
from etl.extract import load_raw, read_header
from etl.schema import map_headers
from etl.transform import standardise_select
from etl.validate import validate

GENERATOR = PROJECT_ROOT / "generator" / "generate_data.py"


@pytest.fixture(scope="session")
def settings():
    return load_settings()


@pytest.fixture()
def con():
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture()
def sql_eval(con):
    """Evaluate a transform expression on a list of raw text values: sql_eval(fn, values) -> list."""
    def _eval(expr_builder, values):
        con.execute("CREATE OR REPLACE TEMP TABLE t (i INTEGER, v VARCHAR)")
        con.executemany("INSERT INTO t VALUES (?, ?)", list(enumerate(values)))
        return [r[0] for r in con.execute(f"SELECT {expr_builder('v')} FROM t ORDER BY i").fetchall()]
    return _eval


@pytest.fixture()
def dims(con):
    """A tiny store and product master inside DuckDB."""
    con.execute("CREATE TABLE dim_store (store_code VARCHAR, is_active VARCHAR)")
    con.execute("INSERT INTO dim_store VALUES ('GGN001', 'Y'), ('GGN007', 'Y')")
    con.execute("CREATE TABLE dim_product (product_code VARCHAR, mrp DECIMAL(12, 2))")
    con.execute("INSERT INTO dim_product VALUES ('SKU00001', 100.00), ('SKU00002', 50.00)")
    return con


@pytest.fixture()
def run_rows(tmp_path, dims, settings):
    """Write a CSV, push it through extract -> standardise -> validate, return validated rows as dicts."""
    def _run(header, rows, store="GGN001", business_date=date(2026, 9, 1)):
        path = tmp_path / "file.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
        hdr = read_header(path)
        hm = map_headers(hdr, settings.contract)
        assert hm.ok, hm.missing_required
        load_raw(dims, path, len(hdr))
        dims.execute(f"CREATE OR REPLACE TEMP TABLE std AS {standardise_select(hm, settings)}")
        validate(dims, settings, store, business_date)
        cur = dims.execute("SELECT * FROM validated ORDER BY row_number")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    return _run


A_HEADER = ["bill_no", "bill_date", "store_code", "product_code", "qty", "unit_price", "discount",
            "net_amount", "payment_mode"]


def a_row(**kw):
    base = {"bill_no": "B1", "bill_date": "2026-09-01 10:00:00", "store_code": "GGN001",
            "product_code": "SKU00001", "qty": "2", "unit_price": "90.00", "discount": "0.00",
            "net_amount": "180.00", "payment_mode": "CASH"}
    base.update(kw)
    return [base[h] for h in A_HEADER]


# ------------------------------------------------------------------ end-to-end helpers
def generate_sample(out: Path, stores: int = 6, days: int = 12, seed: int = 7) -> Path:
    subprocess.run([sys.executable, str(GENERATOR), "--out", str(out), "--stores", str(stores),
                    "--days", str(days), "--seed", str(seed)], check=True, capture_output=True)
    return out


def e2e_settings(data_dir: Path, work_dir: Path, url: str | None = None):
    return load_settings(overrides={
        "paths": {"landing_dir": str(data_dir / "landing"), "master_dir": str(data_dir / "master"),
                  "silver_dir": str(work_dir / "silver"), "log_dir": str(work_dir / "logs"),
                  "report_dir": str(work_dir / "reports")},
        "warehouse": {"url": url or f"sqlite:///{work_dir / 'retail.db'}"},
    })


@pytest.fixture(scope="session")
def sample_data(tmp_path_factory):
    return generate_sample(tmp_path_factory.mktemp("sample_data"))
