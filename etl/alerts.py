"""Missing-store alerts.

A store-day is MISSING when no file with at least one loaded row exists for it (no file at all,
a header-only file, a zero-byte file or a quarantined file all count as missing).
When an expected store has `missing_consecutive_days` (default 2) or more consecutive missing
business dates inside the observed window, an alert is raised.

The check is recomputed from scratch on every run, so it is idempotent: alerts are upserted, and
an alert whose gap has been filled by a late file is removed and logged as ALERT_RESOLVED.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from .config import Settings
from .load import utcnow
from .logging_utils import event
from .warehouse import Warehouse

ALERT_TYPE = "MISSING_CONSECUTIVE_DAYS"


def _d(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def find_missing_runs(present: set[tuple[str, date]], stores: dict[str, date | None], start: date, end: date,
                      min_run: int) -> list[tuple[str, date, date, int]]:
    """Pure function: return (store, first_missing, last_missing, length) for every run >= min_run.

    `stores` maps store_code -> open_date (days before opening are not expected).
    """
    runs = []
    for store, opened in sorted(stores.items()):
        run: list[date] = []
        d = start
        while d <= end + timedelta(days=1):
            expected = d <= end and (opened is None or d >= opened)
            if expected and (store, d) not in present:
                run.append(d)
            else:
                if len(run) >= min_run:
                    runs.append((store, run[0], run[-1], len(run)))
                run = []
            d += timedelta(days=1)
    return runs


def expected_stores(wh: Warehouse, settings: Settings) -> dict[str, date | None]:
    rows = wh.query("SELECT store_code, open_date FROM dim_store WHERE is_active = 'Y'")
    stores = {code: (_d(od) if od else None) for code, od in rows}
    if settings.store_source == "list":
        stores = {c: od for c, od in stores.items() if c in set(settings.store_list)}
    return stores


def update_missing_day_alerts(wh: Warehouse, settings: Settings, run_id: str, log: logging.Logger) -> int:
    """Recompute alerts; returns the number of NEW alerts raised in this run."""
    present_rows = wh.query("SELECT store_code, business_date FROM etl_partition WHERE rows_loaded > 0")
    if not present_rows:
        return 0
    present = {(s, _d(d)) for s, d in present_rows}
    start = min(d for _, d in present)
    end = max(d for _, d in present)
    runs = find_missing_runs(present, expected_stores(wh, settings), start, end,
                             settings.missing_consecutive_days)

    existing = {(s, _d(f)): (_d(l), n) for s, f, l, n in wh.query(
        "SELECT store_code, first_date, last_date, consecutive_days FROM etl_alerts WHERE alert_type = ?",
        (ALERT_TYPE,))}
    current_keys = set()
    new_count = 0
    with wh.transaction():
        for store, first, last, n in runs:
            key = (store, first)
            current_keys.add(key)
            dates = ", ".join((first + timedelta(days=i)).isoformat() for i in range(n))
            details = f"no usable sales file for {n} consecutive business days: {dates}"
            if key not in existing:
                new_count += 1
                wh.execute("""INSERT INTO etl_alerts (alert_type, store_code, first_date, last_date,
                                  consecutive_days, details, raised_at, run_id)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                           (ALERT_TYPE, store, first.isoformat(), last.isoformat(), n, details, utcnow(), run_id))
                event(log, logging.ERROR, "MISSING_DAYS_ALERT", store=store, first_date=first.isoformat(),
                      last_date=last.isoformat(), consecutive_days=n, message=details)
            elif existing[key] != (last, n):
                wh.execute("""UPDATE etl_alerts SET last_date = ?, consecutive_days = ?, details = ?
                              WHERE alert_type = ? AND store_code = ? AND first_date = ?""",
                           (last.isoformat(), n, details, ALERT_TYPE, store, first.isoformat()))
                event(log, logging.WARNING, "MISSING_DAYS_ALERT_UPDATED", store=store,
                      first_date=first.isoformat(), last_date=last.isoformat(), consecutive_days=n)
        for (store, first) in set(existing) - current_keys:
            wh.execute("DELETE FROM etl_alerts WHERE alert_type = ? AND store_code = ? AND first_date = ?",
                       (ALERT_TYPE, store, first.isoformat()))
            event(log, logging.INFO, "ALERT_RESOLVED", store=store, first_date=first.isoformat(),
                  message="late file arrived; gap is now shorter than the alert threshold")
    return new_count
