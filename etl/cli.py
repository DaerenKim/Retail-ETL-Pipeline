"""Command line interface.

    python -m etl init-db                 create warehouse tables + load dimensions
    python -m etl run                     process every new file in the landing area
    python -m etl run --until 2026-09-15  only landing folders up to that date (simulate daily runs)
    python -m etl run --retry-quarantined re-try files quarantined earlier (e.g. after adding a store)
    python -m etl report                  rebuild reports/ from the warehouse
    python -m etl status                  show recent runs and file status counts
    python -m etl reset --yes             delete the warehouse + silver layer (full rerun from scratch)
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import date

from .config import ConfigError, load_settings
from .load import refresh_dimensions
from .logging_utils import close_logging, setup_logging
from .warehouse import Warehouse

DROP_ORDER = ["fact_sales", "etl_rejected_rows", "etl_partition", "etl_alerts", "etl_file_log", "etl_run_log",
              "dim_product", "dim_store"]


def cmd_init_db(args) -> int:
    s = load_settings(args.config)
    wh = Warehouse(s.warehouse_url)
    script = wh.init_schema()
    n_st, n_pr = refresh_dimensions(wh, s)
    print(f"schema applied from {script.name} ({wh.dialect}); dim_store={n_st} dim_product={n_pr}")
    wh.close()
    return 0


def cmd_run(args) -> int:
    from .pipeline import Pipeline

    s = load_settings(args.config)
    until = date.fromisoformat(args.until) if args.until else None
    p = Pipeline(s, console_level=logging.WARNING if args.quiet else logging.INFO)
    try:
        summary = p.run(until=until, retry_quarantined=args.retry_quarantined, make_reports=not args.no_report)
    finally:
        p.close()
    keys = ["run_id", "status", "duration_s", "files_seen", "files_already_processed", "files_new",
            "files_loaded", "files_skipped", "files_quarantined", "files_failed", "files_empty",
            "rows_read", "rows_loaded", "rows_rejected", "log_file"]
    print("\n" + json.dumps({k: summary[k] for k in keys}, indent=2))
    if "reports" in summary:
        r = summary["reports"]
        print(f"reports -> {r['report_dir']}  reject_pct={r['reject_pct']}%  "
              f"reconciled={r['reconciliation']['partitions_within_tolerance']}/{r['reconciliation']['partitions']}  "
              f"criteria={r['success_criteria']}")
    return 0 if summary["status"] != "FAILED" else 1


def cmd_report(args) -> int:
    from .reconcile import write_reports

    s = load_settings(args.config)
    wh = Warehouse(s.warehouse_url)
    log, _ = setup_logging("report", s.log_dir, logging.WARNING)
    last = wh.query("SELECT run_id, status, duration_s FROM etl_run_log ORDER BY started_at DESC LIMIT 1")
    run = {"run_id": last[0][0], "status": last[0][1], "duration_s": last[0][2]} if last else None
    r = write_reports(wh, s, log, run_summary=run)
    close_logging(log)
    wh.close()
    print(json.dumps(r, indent=2, default=str))
    return 0


def cmd_status(args) -> int:
    s = load_settings(args.config)
    wh = Warehouse(s.warehouse_url)
    if "etl_run_log" not in wh.tables():
        print("warehouse not initialised - run: python -m etl init-db")
        return 1
    print("recent runs:")
    for r in wh.query("SELECT run_id, status, files_new, files_loaded, rows_loaded, rows_rejected, duration_s "
                      "FROM etl_run_log ORDER BY started_at DESC LIMIT 10"):
        print("  ", r)
    print("files by status:")
    for r in wh.query("SELECT status, reason_code, files FROM v_file_status ORDER BY files DESC"):
        print("  ", r)
    print("fact_sales rows:", wh.scalar("SELECT COUNT(*) FROM fact_sales"))
    wh.close()
    return 0


def cmd_reset(args) -> int:
    s = load_settings(args.config)
    if not args.yes:
        print("refusing to reset without --yes (this deletes the warehouse and data/silver)")
        return 1
    wh = Warehouse(s.warehouse_url)
    if wh.dialect == "sqlite":
        path = wh.path
        wh.close()
        for suffix in ("", "-wal", "-shm"):
            p = path.with_name(path.name + suffix)
            if p.exists():
                p.unlink()
    else:
        for t in DROP_ORDER:
            wh.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        wh.close()
    if s.silver_dir.exists():
        shutil.rmtree(s.silver_dir)
    print("warehouse and silver layer deleted")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m etl", description="Retail ETL pipeline")
    ap.add_argument("--config", help="path to pipeline.yaml (default: config/pipeline.yaml)")
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="create tables and load dimensions").set_defaults(func=cmd_init_db)
    run = sub.add_parser("run", help="process new landing files")
    run.add_argument("--until", help="only landing folders up to this date (YYYY-MM-DD)")
    run.add_argument("--retry-quarantined", action="store_true", help="re-process quarantined files")
    run.add_argument("--no-report", action="store_true", help="skip writing reports/")
    run.add_argument("--quiet", action="store_true", help="console shows warnings and errors only")
    run.set_defaults(func=cmd_run)
    sub.add_parser("report", help="rebuild reports from the warehouse").set_defaults(func=cmd_report)
    sub.add_parser("status", help="show run history").set_defaults(func=cmd_status)
    reset = sub.add_parser("reset", help="delete warehouse and silver layer")
    reset.add_argument("--yes", action="store_true")
    reset.set_defaults(func=cmd_reset)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
