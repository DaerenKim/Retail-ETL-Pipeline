"""Reconciliation and data-quality reporting.

Reconciliation re-reads every source file that currently owns a partition with Python's csv module -
a completely different code path from the DuckDB load - and checks, per store per day:

    expected_loaded_net = sum(net_amount of every row in the file) - sum(net_amount of rejected rows)
    variance_pct        = |warehouse_net - expected_loaded_net| / |expected_loaded_net| * 100

`within_tolerance` is variance_pct <= thresholds.reconciliation_tolerance_pct (0.5%).
`raw_variance_pct` (warehouse vs. ALL source rows, rejects included) is reported for information:
it shows how much money the rejected rows represent.
"""
from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .logging_utils import event
from .warehouse import Warehouse


def _money(text: str) -> float | None:
    try:
        return float(text.strip().replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _write_csv(path: Path, header: list[str], rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def reconcile(wh: Warehouse, settings: Settings) -> tuple[list[list], dict]:
    tol = settings.thresholds["reconciliation_tolerance_pct"]
    parts = wh.query("""
        SELECT p.store_code, p.business_date, f.file_id, f.file_path, f.column_mapping
        FROM etl_partition p JOIN etl_file_log f ON f.file_id = p.file_id
        ORDER BY p.store_code, p.business_date""")
    wh_totals = {(s, str(d)[:10]): (n, float(v or 0)) for s, d, n, v in wh.query(
        "SELECT store_code, business_date, COUNT(*), SUM(net_amount) FROM fact_sales GROUP BY 1, 2")}
    rejected = defaultdict(set)
    for fid, rn in wh.query("SELECT file_id, row_number FROM etl_rejected_rows"):
        rejected[fid].add(rn)

    rows, n_ok, max_var, total_src, total_wh, total_rej_net = [], 0, 0.0, 0.0, 0.0, 0.0
    for store, bdate, fid, fpath, mapping in parts:
        bdate = str(bdate)[:10]
        mapping = mapping if isinstance(mapping, dict) else json.loads(mapping)
        net_header = mapping["net_amount"]
        src_rows = src_net = rej_net = 0.0
        unparseable = 0
        with open(settings.landing_dir / fpath, newline="", encoding="utf-8-sig") as fh:
            reader = csv.reader(fh)
            idx = next(reader).index(net_header)
            for i, rec in enumerate(reader, start=1):
                src_rows += 1
                v = _money(rec[idx]) if idx < len(rec) else None
                if v is None:
                    unparseable += 1
                    continue
                src_net += v
                if i in rejected[fid]:
                    rej_net += v
        expected = round(src_net - rej_net, 2)
        wh_rows, wh_net = wh_totals.get((store, bdate), (0, 0.0))
        wh_net = round(wh_net, 2)
        var = abs(wh_net - expected) / abs(expected) * 100 if expected else (0.0 if wh_net == 0 else 100.0)
        raw_var = abs(src_net - wh_net) / abs(src_net) * 100 if src_net else 0.0
        ok = var <= tol
        n_ok += ok
        max_var = max(max_var, var)
        total_src += src_net
        total_wh += wh_net
        total_rej_net += rej_net
        rows.append([store, bdate, fpath, int(src_rows), round(src_net, 2), len(rejected[fid]), round(rej_net, 2),
                     unparseable, expected, wh_rows, wh_net, round(wh_net - expected, 2), round(var, 4),
                     round(raw_var, 4), "Y" if ok else "N"])
    summary = {
        "partitions": len(rows), "partitions_within_tolerance": n_ok,
        "pct_within_tolerance": round(100 * n_ok / len(rows), 2) if rows else 100.0,
        "max_variance_pct": round(max_var, 4), "tolerance_pct": tol,
        "source_net_total": round(total_src, 2), "rejected_net_total": round(total_rej_net, 2),
        "warehouse_net_total": round(total_wh, 2),
        "partitions_within_tolerance_raw_basis": sum(1 for r in rows if r[13] <= tol),
    }
    return rows, summary


def dq_tables(wh: Warehouse) -> dict:
    rows_read = wh.scalar("SELECT COALESCE(SUM(rows_read), 0) FROM etl_file_log "
                          "WHERE status IN ('LOADED', 'QUARANTINED')") or 0
    rows_loaded = wh.scalar("SELECT COUNT(*) FROM fact_sales") or 0
    rejects = wh.query("SELECT reason_code, COUNT(*), COUNT(DISTINCT file_id) FROM etl_rejected_rows "
                       "GROUP BY reason_code ORDER BY 2 DESC")
    total_rej = sum(r[1] for r in rejects)
    by_store = wh.query("""
        SELECT store_code, SUM(rows_read), SUM(rows_loaded), SUM(rows_rejected), COUNT(*)
        FROM etl_file_log WHERE status IN ('LOADED', 'QUARANTINED')
        GROUP BY store_code ORDER BY store_code""")
    by_day = wh.query("""
        SELECT business_date, SUM(rows_read), SUM(rows_loaded), SUM(rows_rejected), COUNT(*)
        FROM etl_file_log WHERE status IN ('LOADED', 'QUARANTINED')
        GROUP BY business_date ORDER BY business_date""")
    file_status = wh.query("SELECT status, reason_code, files, rows_read FROM v_file_status ORDER BY files DESC")
    alerts = wh.query("SELECT store_code, first_date, last_date, consecutive_days, raised_at, details "
                      "FROM etl_alerts ORDER BY store_code, first_date")
    return {"rows_read": int(rows_read), "rows_loaded": int(rows_loaded), "rows_rejected": int(total_rej),
            "reject_pct": round(100 * total_rej / rows_read, 3) if rows_read else 0.0,
            "rejects": rejects, "by_store": by_store, "by_day": by_day, "file_status": file_status,
            "alerts": alerts}


def write_reports(wh: Warehouse, settings: Settings, log: logging.Logger, run_summary: dict | None = None) -> dict:
    out = settings.report_dir
    out.mkdir(parents=True, exist_ok=True)
    rec_rows, rec = reconcile(wh, settings)
    _write_csv(out / "reconciliation.csv",
               ["store_code", "business_date", "source_file", "source_rows", "source_net_all_rows",
                "rejected_rows", "rejected_net", "unparseable_amounts", "expected_loaded_net",
                "warehouse_rows", "warehouse_net", "difference", "variance_pct", "raw_variance_pct",
                "within_tolerance"], rec_rows)

    dq = dq_tables(wh)
    rr = dq["rows_read"] or 1
    _write_csv(out / "dq_summary.csv", ["reason_code", "rejected_rows", "pct_of_rows_read", "files_affected"],
               [[c, n, round(100 * n / rr, 3), f] for c, n, f in dq["rejects"]])
    _write_csv(out / "dq_by_store.csv", ["store_code", "rows_read", "rows_loaded", "rows_rejected", "reject_pct", "files"],
               [[s, r, l, j, round(100 * j / r, 3) if r else 0, n] for s, r, l, j, n in dq["by_store"]])
    _write_csv(out / "dq_by_day.csv", ["business_date", "rows_read", "rows_loaded", "rows_rejected", "reject_pct", "files"],
               [[str(d)[:10], r, l, j, round(100 * j / r, 3) if r else 0, n] for d, r, l, j, n in dq["by_day"]])
    _write_csv(out / "file_status.csv", ["status", "reason_code", "files", "rows_read"], dq["file_status"])
    _write_csv(out / "alerts.csv", ["store_code", "first_date", "last_date", "consecutive_days", "raised_at", "details"],
               dq["alerts"])

    # The performance target is about a FULL rerun: the latest run that processed every file it saw.
    full = wh.query("SELECT run_id, duration_s, files_seen, rows_read FROM etl_run_log "
                    "WHERE files_seen > 0 AND files_new = files_seen AND status <> 'FAILED' "
                    "ORDER BY started_at DESC LIMIT 1")
    full_run = ({"run_id": full[0][0], "duration_s": full[0][1], "files": full[0][2], "rows_read": full[0][3]}
                if full else None)
    duration = full_run["duration_s"] if full_run else None
    criteria = {
        "reconciliation_within_0_5_pct": rec["partitions_within_tolerance"] == rec["partitions"],
        "reject_rate_below_2_pct": dq["reject_pct"] < settings.thresholds["max_run_reject_pct"],
        "run_under_10_minutes": duration is not None and duration < 600,
    }
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "warehouse": wh.dialect,
        "last_run": run_summary,
        "last_full_run": full_run,
        "rows_read": dq["rows_read"], "rows_loaded": dq["rows_loaded"], "rows_rejected": dq["rows_rejected"],
        "reject_pct": dq["reject_pct"],
        "rejects_by_reason": {c: n for c, n, _ in dq["rejects"]},
        "file_status": {f"{s}{'/' + r if r else ''}": n for s, r, n, _ in dq["file_status"]},
        "reconciliation": rec,
        "alerts": [{"store_code": a[0], "first_date": str(a[1])[:10], "last_date": str(a[2])[:10],
                    "consecutive_days": a[3]} for a in dq["alerts"]],
        "success_criteria": criteria,
    }
    (out / "run_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    (out / "DATA_QUALITY_SUMMARY.md").write_text(_markdown(summary, dq))
    event(log, logging.INFO, "REPORTS_WRITTEN", report_dir=str(out), reject_pct=dq["reject_pct"],
          reconciliation_ok=criteria["reconciliation_within_0_5_pct"],
          max_variance_pct=rec["max_variance_pct"])
    return {"report_dir": str(out), "reject_pct": dq["reject_pct"], "success_criteria": criteria,
            "reconciliation": rec}


def _markdown(summary: dict, dq: dict) -> str:
    rec = summary["reconciliation"]
    run = summary.get("last_run") or {}
    full = summary.get("last_full_run") or {}
    lines = [
        "# Data Quality Summary", "",
        f"_Generated {summary['generated_at']} from the `{summary['warehouse']}` warehouse "
        f"(last run `{run.get('run_id', 'n/a')}`, {run.get('duration_s', 'n/a')} s)._", "",
        "## Headline", "",
        "| Metric | Value |", "|---|---|",
        f"| Last full rerun | {full.get('duration_s', 'n/a')} s for {full.get('files', 0):,} files (target < 600 s) |",
        f"| Rows read (current files) | {summary['rows_read']:,} |",
        f"| Rows loaded | {summary['rows_loaded']:,} |",
        f"| Rows rejected | {summary['rows_rejected']:,} |",
        f"| **Reject rate** | **{summary['reject_pct']}%** (target < 2%) |",
        f"| Partitions reconciled within {rec['tolerance_pct']}% | {rec['partitions_within_tolerance']:,} / {rec['partitions']:,} |",
        f"| Max reconciliation variance | {rec['max_variance_pct']}% |",
        f"| Source net (all rows) | ${rec['source_net_total']:,.2f} |",
        f"| Rejected net | ${rec['rejected_net_total']:,.2f} |",
        f"| Warehouse net | ${rec['warehouse_net_total']:,.2f} |",
        "", "## Rejected rows by reason", "",
        "| Reason code | Rows | % of rows read | Files affected |", "|---|---:|---:|---:|",
    ]
    rr = summary["rows_read"] or 1
    lines += [f"| `{c}` | {n:,} | {100 * n / rr:.3f}% | {f:,} |" for c, n, f in dq["rejects"]]
    lines += ["", "## Files by status", "", "| Status | Reason | Files |", "|---|---|---:|"]
    lines += [f"| {s} | {r or ''} | {n:,} |" for s, r, n, _ in dq["file_status"]]
    lines += ["", "## Missing-store alerts", ""]
    if dq["alerts"]:
        lines += ["| Store | First missing | Last missing | Days |", "|---|---|---|---:|"]
        lines += [f"| {a[0]} | {str(a[1])[:10]} | {str(a[2])[:10]} | {a[3]} |" for a in dq["alerts"]]
    else:
        lines.append("None.")
    worst = sorted(dq["by_store"], key=lambda r: -(r[3] / r[1] if r[1] else 0))[:5]
    lines += ["", "## Stores with the highest reject rate", "", "| Store | Rows read | Rejected | Reject % |",
              "|---|---:|---:|---:|"]
    lines += [f"| {s} | {r:,} | {j:,} | {100 * j / r:.2f}% |" for s, r, _, j, _ in worst if r]
    lines += ["", "## Success criteria", ""]
    lines += [f"- {'✅' if v else '❌'} {k.replace('_', ' ')}" for k, v in summary["success_criteria"].items()]
    return "\n".join(lines) + "\n"
