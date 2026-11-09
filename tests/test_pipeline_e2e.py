"""End-to-end tests: generate a small dataset, run the real pipeline, compare with the generator's
ground truth (the answer key the pipeline never sees)."""
from __future__ import annotations

import csv
import logging
import os
import shutil
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from etl.pipeline import Pipeline
from etl.warehouse import Warehouse
from tests.conftest import e2e_settings


def run_pipeline(settings, **kw):
    p = Pipeline(settings, console_level=logging.CRITICAL)
    try:
        return p.run(**kw)
    finally:
        p.close()


def truth(data: Path, name: str) -> list[dict]:
    with open(data / "_ground_truth" / name, newline="") as fh:
        return list(csv.DictReader(fh))


def warehouse_state(settings) -> dict:
    wh = Warehouse(settings.warehouse_url)
    try:
        return {
            "facts": {(s, str(d)[:10]): (n, round(float(v), 2)) for s, d, n, v in wh.query(
                "SELECT store_code, business_date, COUNT(*), SUM(net_amount) FROM fact_sales GROUP BY 1, 2")},
            "rejects": dict(wh.query("SELECT reason_code, COUNT(*) FROM etl_rejected_rows GROUP BY 1")),
            "alerts": sorted((s, str(f)[:10], str(l)[:10]) for s, f, l in
                             wh.query("SELECT store_code, first_date, last_date FROM etl_alerts")),
            "file_status": dict(wh.query("SELECT status, COUNT(*) FROM etl_file_log GROUP BY 1")),
            "n_facts": wh.scalar("SELECT COUNT(*) FROM fact_sales"),
        }
    finally:
        wh.close()


def assert_matches_ground_truth(state, data: Path):
    expected = {(r["store_code"], r["business_date"]): (int(r["expected_lines"]), float(r["expected_net_amount"]))
                for r in truth(data, "expected_daily_totals.csv") if int(r["expected_lines"])}
    assert set(state["facts"]) == set(expected)
    for key, (lines, net) in expected.items():
        got_lines, got_net = state["facts"][key]
        assert got_lines == lines, key
        assert abs(got_net - net) < 0.01, key
    injected = Counter(r["anomaly_code"] for r in truth(data, "injected_anomalies.csv") if r["superseded"] == "N")
    assert state["rejects"] == dict(injected)
    alerts = sorted((r["store_code"], r["first_missing_date"], r["last_missing_date"])
                    for r in truth(data, "expected_alerts.csv"))
    assert state["alerts"] == alerts


@pytest.fixture()
def full_run(sample_data, tmp_path):
    s = e2e_settings(sample_data, tmp_path)
    summary = run_pipeline(s)
    return s, summary


def test_full_run_matches_ground_truth(full_run, sample_data):
    s, summary = full_run
    assert summary["status"] in ("SUCCESS", "SUCCESS_WITH_WARNINGS")
    assert_matches_ground_truth(warehouse_state(s), sample_data)


def test_rerun_is_idempotent(full_run):
    s, _ = full_run
    before = warehouse_state(s)
    again = run_pipeline(s)
    assert again["files_new"] == 0 and again["rows_loaded"] == 0
    assert warehouse_state(s) == before


def test_file_level_events_are_handled(full_run, sample_data):
    s, _ = full_run
    manifest = Counter(r["event"] for r in truth(sample_data, "file_manifest.csv"))
    status = warehouse_state(s)["file_status"]
    assert status["SKIPPED_DUPLICATE"] == manifest["DUPLICATE_DELIVERY"]
    assert status["SUPERSEDED"] == manifest["REVISED_V2"]           # every v1 replaced by its v2
    assert status["QUARANTINED"] == manifest["UNKNOWN_STORE"]
    assert status["EMPTY"] == manifest["HEADER_ONLY"]
    assert status["FAILED"] == manifest["ZERO_BYTE"]


def test_reconciliation_report(full_run):
    s, summary = full_run
    rec = summary["reports"]["reconciliation"]
    assert rec["partitions"] > 0
    assert rec["partitions_within_tolerance"] == rec["partitions"]
    assert rec["max_variance_pct"] == 0
    for name in ["reconciliation.csv", "dq_summary.csv", "run_summary.json", "DATA_QUALITY_SUMMARY.md"]:
        assert (s.report_dir / name).exists()


def test_silver_parquet_matches_warehouse(full_run):
    s, _ = full_run
    total = duckdb.sql(f"SELECT round(sum(net_amount), 2), count(*) FROM read_parquet("
                       f"'{s.silver_dir}/sales/*/*/*.parquet', hive_partitioning = true)").fetchone()
    wh = Warehouse(s.warehouse_url)
    n, v = wh.query("SELECT COUNT(*), ROUND(SUM(net_amount), 2) FROM fact_sales")[0]
    wh.close()
    assert total[1] == n and abs(float(total[0]) - v) < 0.01


def test_daily_incremental_runs_equal_one_full_run(sample_data, tmp_path):
    """Process the landing area one arrival folder at a time; final state must equal a single full run."""
    s = e2e_settings(sample_data, tmp_path)
    folders = sorted(p.name for p in (sample_data / "landing").iterdir())
    for folder in folders:
        run_pipeline(s, until=date.fromisoformat(folder), make_reports=False)
    assert_matches_ground_truth(warehouse_state(s), sample_data)


def test_crash_mid_load_leaves_partition_untouched(sample_data, tmp_path, monkeypatch):
    """If inserting rows fails, the transaction rolls back: no partial partition, file not registered."""
    s = e2e_settings(sample_data, tmp_path)
    first = sorted((sample_data / "landing").iterdir())[0].name
    import etl.load as load_mod

    original = load_mod.fetch_valid_rows
    calls = {"n": 0}

    def boom(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated crash")
        return original(*a, **kw)

    monkeypatch.setattr(load_mod, "fetch_valid_rows", boom)
    with pytest.raises(RuntimeError):
        run_pipeline(s, until=date.fromisoformat(first), make_reports=False)
    wh = Warehouse(s.warehouse_url)
    loaded_files = wh.scalar("SELECT COUNT(*) FROM etl_file_log WHERE status = 'LOADED'")
    partitions = wh.scalar("SELECT COUNT(DISTINCT store_code || business_date) FROM fact_sales")
    wh.close()
    assert loaded_files == 1 and partitions == 1           # only the first file made it

    monkeypatch.setattr(load_mod, "fetch_valid_rows", original)
    run_pipeline(s, make_reports=False)                    # recovery: just run again
    assert_matches_ground_truth(warehouse_state(s), sample_data)


def test_retry_quarantined_after_adding_store(sample_data, tmp_path):
    data = tmp_path / "data"
    shutil.copytree(sample_data, data)
    s = e2e_settings(data, tmp_path / "work")
    run_pipeline(s, make_reports=False)
    unknown = truth(data, "file_manifest.csv")
    unknown_store = next(r["store_code"] for r in unknown if r["event"] == "UNKNOWN_STORE")

    with open(data / "master" / "stores.csv", "a", newline="") as fh:
        csv.writer(fh).writerow([unknown_store, "New store", "Sector 102", "West", "Gurgaon", "Haryana",
                                 "Express", "POS-v3", "2026-09-01", 2000, "Y"])
    summary = run_pipeline(s, retry_quarantined=True, make_reports=False)
    assert summary["files_quarantined"] == 0 and summary["files_loaded"] > 0
    wh = Warehouse(s.warehouse_url)
    assert wh.scalar("SELECT COUNT(*) FROM fact_sales WHERE store_code = ?", (unknown_store,)) > 0
    assert wh.scalar("SELECT COUNT(*) FROM etl_file_log WHERE status = 'QUARANTINED'") == 0
    wh.close()


def test_stale_v1_after_v2_is_skipped(sample_data, tmp_path):
    data = tmp_path / "data"
    shutil.copytree(sample_data, data)
    v2 = next(r for r in truth(data, "file_manifest.csv") if r["event"] == "REVISED_V2")
    v1_name = v2["file_name"].replace("_v2", "")
    v1_src = next((data / "landing").glob(f"*/{v1_name}"))
    s = e2e_settings(data, tmp_path / "work")
    run_pipeline(s, make_reports=False)
    # the old v1 file shows up again (modified so its checksum is new) AFTER v2 was loaded
    late = data / "landing" / (date.fromisoformat(v2["landing_folder"]) + timedelta(days=5)).isoformat()
    late.mkdir(exist_ok=True)
    (late / v1_name).write_bytes(v1_src.read_bytes() + b"\n")
    summary = run_pipeline(s, make_reports=False)
    assert summary["statuses"].get("SKIPPED_STALE_VERSION") == 1
    assert_matches_ground_truth(warehouse_state(s), data)


def test_schema_missing_column_fails_file(sample_data, tmp_path):
    data = tmp_path / "data"
    shutil.copytree(sample_data, data)
    target = next((data / "landing").glob("*/GGN*_2026????.csv"))
    lines = target.read_text().splitlines()
    header = lines[0].split(",")
    drop = next(i for i, h in enumerate(header) if h.strip().lower() in ("net_amount",))
    target.write_text("\n".join(",".join(c for i, c in enumerate(l.split(",")) if i != drop) for l in lines) + "\n")
    s = e2e_settings(data, tmp_path / "work")
    run_pipeline(s, make_reports=False)
    wh = Warehouse(s.warehouse_url)
    reason = wh.scalar("SELECT reason_code FROM etl_file_log WHERE file_name = ? AND landing_folder = ?",
                       (target.name, target.parent.name))
    wh.close()
    assert reason == "SCHEMA_MISSING_COLUMN"


@pytest.mark.skipif(not os.environ.get("RETAIL_TEST_PG_URL"), reason="set RETAIL_TEST_PG_URL to run on PostgreSQL")
def test_postgres_warehouse_matches_ground_truth(sample_data, tmp_path):
    url = os.environ["RETAIL_TEST_PG_URL"]
    wh = Warehouse(url)
    for t in ["fact_sales", "etl_rejected_rows", "etl_partition", "etl_alerts", "etl_file_log", "etl_run_log",
              "dim_product", "dim_store"]:
        wh.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    wh.close()
    s = e2e_settings(sample_data, tmp_path, url=url)
    run_pipeline(s)
    assert_matches_ground_truth(warehouse_state(s), sample_data)
    again = run_pipeline(s)
    assert again["files_new"] == 0
