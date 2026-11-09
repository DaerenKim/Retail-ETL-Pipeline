"""Orchestration: discover -> fingerprint -> (skip?) -> extract -> map schema -> standardise ->
validate -> load partition -> silver Parquet; then alerts and reports for the whole run."""
from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from . import alerts, reconcile
from .config import Settings
from .discover import LandingFile, discover_files, fingerprint
from .extract import EmptyFileError, UnreadableFileError, load_raw, read_header
from .load import (FileRecord, current_partition, dims_to_duckdb, load_partition, quarantine_file,
                   refresh_dimensions, upsert_file_log, utcnow, write_silver)
from .logging_utils import close_logging, event, setup_logging
from .schema import map_headers
from .transform import standardise_select
from .validate import validate
from .warehouse import Warehouse

INFO, WARNING, ERROR, DEBUG = logging.INFO, logging.WARNING, logging.ERROR, logging.DEBUG
DONE_STATUSES = ("LOADED", "SUPERSEDED", "EMPTY", "QUARANTINED")


@dataclass
class RunStats:
    files_seen: int = 0
    files_known: int = 0           # already in the registry with the same path + checksum
    files_new: int = 0
    files_loaded: int = 0
    files_skipped: int = 0
    files_quarantined: int = 0
    files_failed: int = 0
    files_empty: int = 0
    rows_read: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    statuses: Counter = field(default_factory=Counter)


class Pipeline:
    def __init__(self, settings: Settings, console_level: int = INFO):
        self.settings = settings
        now = datetime.now(timezone.utc)
        self.run_id = f"{now:%Y%m%dT%H%M%S}{now.microsecond // 1000:03d}"
        self.log, self.log_path = setup_logging(self.run_id, settings.log_dir, console_level)
        self.wh = Warehouse(settings.warehouse_url)
        self.con = duckdb.connect()            # in-memory engine for parsing / validation
        self.stats = RunStats()
        self.known_stores: set[str] = set()
        self.last_signature: dict[str, str] = {}

    # ------------------------------------------------------------------------------ public
    def run(self, until: date | None = None, retry_quarantined: bool = False, make_reports: bool = True) -> dict:
        t0 = time.perf_counter()
        s = self.settings
        if "fact_sales" not in self.wh.tables():
            self.wh.init_schema()
        self.wh.execute("INSERT INTO etl_run_log (run_id, started_at, status, until_date, log_file) "
                        "VALUES (?, ?, 'RUNNING', ?, ?)",
                        (self.run_id, utcnow(), until.isoformat() if until else None, str(self.log_path)))
        event(self.log, INFO, "RUN_STARTED", warehouse=self.wh.dialect, landing_dir=str(s.landing_dir),
              until=until.isoformat() if until else None)
        status = "FAILED"
        summary: dict = {}
        try:
            n_st, n_pr = refresh_dimensions(self.wh, s)
            dims_to_duckdb(self.wh, self.con)
            self.known_stores = {r[0] for r in self.wh.query("SELECT store_code FROM dim_store")}
            self._load_signatures()
            event(self.log, INFO, "DIMENSIONS_REFRESHED", stores=n_st, products=n_pr)

            files = discover_files(s.landing_dir, s.file_patterns, until)
            fingerprint(files)
            self.stats.files_seen = len(files)
            seen = {(p, h): st for p, h, st in self.wh.query("SELECT file_path, sha256, status FROM etl_file_log")}
            event(self.log, INFO, "FILES_DISCOVERED", files=len(files))

            for f in files:
                key = (f"{f.landing_folder}/{f.file_name}", f.sha256)
                prior = seen.get(key)
                if prior is not None and not (retry_quarantined and prior == "QUARANTINED"):
                    self.stats.files_known += 1
                    continue
                self.stats.files_new += 1
                self._process(f)

            new_alerts = alerts.update_missing_day_alerts(self.wh, s, self.run_id, self.log)
            run_reject_pct = 100 * self.stats.rows_rejected / self.stats.rows_read if self.stats.rows_read else 0.0
            status = "SUCCESS"
            if self.stats.files_failed or run_reject_pct > s.thresholds["max_run_reject_pct"] or new_alerts:
                status = "SUCCESS_WITH_WARNINGS"
            if run_reject_pct > s.thresholds["max_run_reject_pct"]:
                event(self.log, WARNING, "RUN_REJECT_RATE_HIGH", reject_pct=round(run_reject_pct, 3),
                      limit=s.thresholds["max_run_reject_pct"])
        except Exception as exc:
            event(self.log, ERROR, "RUN_FAILED", error=repr(exc))
            raise
        finally:
            duration = time.perf_counter() - t0
            st = self.stats
            self.wh.execute("""
                UPDATE etl_run_log SET finished_at = ?, status = ?, files_seen = ?, files_new = ?,
                    files_loaded = ?, files_skipped = ?, files_quarantined = ?, files_failed = ?,
                    rows_read = ?, rows_loaded = ?, rows_rejected = ?, duration_s = ?
                WHERE run_id = ?""",
                (utcnow(), status, st.files_seen, st.files_new, st.files_loaded, st.files_skipped,
                 st.files_quarantined, st.files_failed, st.rows_read, st.rows_loaded, st.rows_rejected,
                 round(duration, 2), self.run_id))
            summary = {
                "run_id": self.run_id, "status": status, "duration_s": round(duration, 2),
                "files_seen": st.files_seen, "files_already_processed": st.files_known,
                "files_new": st.files_new, "files_loaded": st.files_loaded, "files_skipped": st.files_skipped,
                "files_quarantined": st.files_quarantined, "files_failed": st.files_failed,
                "files_empty": st.files_empty, "rows_read": st.rows_read, "rows_loaded": st.rows_loaded,
                "rows_rejected": st.rows_rejected, "statuses": dict(st.statuses), "log_file": str(self.log_path),
            }
            event(self.log, INFO if status != "FAILED" else ERROR, "RUN_FINISHED", **summary)

        if make_reports:
            report = reconcile.write_reports(self.wh, self.settings, self.log, run_summary=summary)
            summary["reports"] = report
        return summary

    def close(self) -> None:
        self.con.close()
        self.wh.close()
        close_logging(self.log)

    # ------------------------------------------------------------------------------ per file
    def _load_signatures(self) -> None:
        rows = self.wh.query("SELECT store_code, layout_signature FROM etl_file_log "
                             "WHERE status IN ('LOADED', 'SUPERSEDED') AND layout_signature IS NOT NULL "
                             "ORDER BY file_id")
        self.last_signature = {store: sig for store, sig in rows}

    def _record(self, f: LandingFile, status: str, reason: str | None = None, message: str | None = None,
                **kw) -> FileRecord:
        return FileRecord(run_id=self.run_id, file_name=f.file_name, landing_folder=f.landing_folder,
                          file_path=f"{f.landing_folder}/{f.file_name}", store_code=f.store_code,
                          business_date=f.business_date.isoformat() if f.business_date else None,
                          file_version=f.version, sha256=f.sha256, bytes=f.bytes, status=status,
                          reason_code=reason, message=message, **kw)

    def _finish_without_load(self, f: LandingFile, status: str, reason: str, message: str,
                             level: int = WARNING, **kw) -> None:
        rec = self._record(f, status, reason, message, **kw)
        upsert_file_log(self.wh, rec)
        self.stats.statuses[status] += 1
        if status == "FAILED":
            self.stats.files_failed += 1
        elif status.startswith("SKIPPED"):
            self.stats.files_skipped += 1
        elif status == "EMPTY":
            self.stats.files_empty += 1
        event(self.log, level, f"FILE_{status}", file=f.file_name, folder=f.landing_folder,
              store=f.store_code, business_date=rec.business_date, reason=reason, message=message)

    def _process(self, f: LandingFile) -> None:
        s = self.settings
        t0 = time.perf_counter()

        # ---- file-level gates (cheap checks first)
        if f.store_code is None:
            return self._finish_without_load(f, "FAILED", "UNPARSEABLE_FILENAME",
                                             "filename matches no configured pattern")
        if f.bytes == 0:
            return self._finish_without_load(f, "FAILED", "ZERO_BYTE_FILE", "file is empty (0 bytes)")
        # Same bytes for the same store-day = a re-delivery. (Same bytes for a DIFFERENT store-day can
        # legitimately happen, e.g. two header-only files, so the partition is part of the identity.)
        dup = self.wh.query(f"SELECT file_name, landing_folder FROM etl_file_log WHERE sha256 = ? "
                            f"AND store_code = ? AND business_date = ? AND file_path <> ? "
                            f"AND status IN ({', '.join(repr(x) for x in DONE_STATUSES)}) LIMIT 1",
                            (f.sha256, f.store_code, f.business_date.isoformat() if f.business_date else None,
                             f"{f.landing_folder}/{f.file_name}"))
        if dup:
            return self._finish_without_load(
                f, "SKIPPED_DUPLICATE", "DUPLICATE_FILE",
                f"same content as {dup[0][1]}/{dup[0][0]}", level=INFO)
        bdate = f.business_date.isoformat()
        current = current_partition(self.wh, f.store_code, bdate)
        if current and f.version < current[1]:
            return self._finish_without_load(
                f, "SKIPPED_STALE_VERSION", "STALE_VERSION",
                f"v{f.version} arrived after v{current[1]} was loaded", level=WARNING)

        # ---- extract + schema mapping
        try:
            header = read_header(f.path)
        except EmptyFileError as exc:
            return self._finish_without_load(f, "FAILED", "EMPTY_FILE", str(exc))
        except UnreadableFileError as exc:
            return self._finish_without_load(f, "FAILED", "UNREADABLE_FILE", str(exc), level=ERROR)

        hm = map_headers(header, s.contract, fuzzy=s.fuzzy_auto_map, cutoff=s.fuzzy_cutoff)
        mapping_json = {c: hm.source_header(c) for c in hm.mapping}
        if not hm.ok:
            return self._finish_without_load(
                f, "FAILED", "SCHEMA_MISSING_COLUMN", f"missing required: {', '.join(hm.missing_required)}",
                level=ERROR, layout_signature=hm.signature, column_mapping=mapping_json)
        if hm.auto_mapped:
            event(self.log, WARNING, "SCHEMA_AUTO_MAPPED", file=f.file_name, store=f.store_code,
                  mapped={k: v for k, v in hm.auto_mapped.items()})
        prev_sig = self.last_signature.get(f.store_code)
        if prev_sig and prev_sig != hm.signature:
            event(self.log, WARNING, "SCHEMA_CHANGED", file=f.file_name, store=f.store_code,
                  business_date=bdate, extra_columns=hm.extras, mapping=mapping_json)
        if hm.extras:
            event(self.log, DEBUG, "SCHEMA_DRIFT_EXTRA_COLUMN", file=f.file_name, columns=hm.extras)

        try:
            n_rows = load_raw(self.con, f.path, len(header))
        except UnreadableFileError as exc:
            return self._finish_without_load(f, "FAILED", "UNREADABLE_FILE", str(exc), level=ERROR,
                                             layout_signature=hm.signature, column_mapping=mapping_json)
        if n_rows == 0:
            return self._finish_without_load(f, "EMPTY", "EMPTY_FILE", "header only, no data rows",
                                             layout_signature=hm.signature, column_mapping=mapping_json)

        # ---- standardise + validate (DuckDB)
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE std AS {standardise_select(hm, s)}")
        validate(self.con, s, f.store_code, f.business_date)
        rows_read, rejected, src_net, ok_net = self.con.execute("""
            SELECT count(*), count(reason_code), CAST(sum(net_amount) AS DOUBLE),
                   CAST(sum(net_amount) FILTER (WHERE reason_code IS NULL) AS DOUBLE)
            FROM validated""").fetchone()
        loaded = rows_read - rejected
        reject_pct = 100 * rejected / rows_read
        rec = self._record(f, "LOADED", layout_signature=hm.signature, column_mapping=mapping_json,
                           rows_read=rows_read, rows_loaded=loaded, rows_rejected=rejected,
                           source_net_amount=round(src_net or 0, 2), loaded_net_amount=round(ok_net or 0, 2))
        self.stats.rows_read += rows_read

        # ---- quarantine: unknown store, or too many bad rows
        if f.store_code not in self.known_stores:
            rec.status, rec.reason_code, rec.rows_loaded, rec.rows_rejected = "QUARANTINED", "UNKNOWN_STORE_FILE", 0, rows_read
            rec.loaded_net_amount = 0.0
            rec.message = f"store {f.store_code} is not in the store master; add it and run with --retry-quarantined"
            quarantine_file(self.wh, self.con, rec, "UNKNOWN_STORE", f"file store {f.store_code} not in dim_store")
            return self._quarantined(f, rec)
        if reject_pct > s.thresholds["max_file_reject_pct"]:
            rec.status, rec.reason_code, rec.rows_loaded, rec.rows_rejected = "QUARANTINED", "REJECT_RATE_EXCEEDED", 0, rows_read
            rec.loaded_net_amount = 0.0
            rec.message = f"{reject_pct:.1f}% rejected > limit {s.thresholds['max_file_reject_pct']}%"
            # valid rows of a quarantined file are kept as rejects too, so the file is fully accounted for
            quarantine_file(self.wh, self.con, rec, override_detail=rec.message,
                            fill_reason="REJECT_RATE_EXCEEDED")
            return self._quarantined(f, rec)

        # ---- load (one transaction) + silver
        file_id, prev_id = load_partition(self.wh, self.con, rec)
        if s.write_silver:
            write_silver(self.con, s, rec)
        self.last_signature[f.store_code] = hm.signature
        self.stats.files_loaded += 1
        self.stats.rows_loaded += loaded
        self.stats.rows_rejected += rejected
        self.stats.statuses["LOADED"] += 1
        if prev_id:
            # the replaced file's rows no longer count (they were deleted from the partition)
            prev_rows = self.wh.query("SELECT rows_read, rows_loaded, rows_rejected, run_id FROM etl_file_log "
                                      "WHERE file_id = ?", (prev_id,))[0]
            if prev_rows[3] == self.run_id:
                self.stats.rows_read -= prev_rows[0]
                self.stats.rows_loaded -= prev_rows[1]
                self.stats.rows_rejected -= prev_rows[2]
            event(self.log, WARNING, "PARTITION_REPLACED", file=f.file_name, store=f.store_code,
                  business_date=bdate, previous_file_id=prev_id, file_id=file_id)
        reasons = dict(self.con.execute("SELECT reason_code, count(*) FROM validated WHERE reason_code IS NOT NULL "
                                        "GROUP BY 1 ORDER BY 2 DESC").fetchall())
        event(self.log, INFO, "FILE_LOADED", file=f.file_name, folder=f.landing_folder, store=f.store_code,
              business_date=bdate, version=f.version, rows_read=rows_read, rows_loaded=loaded,
              rows_rejected=rejected, reject_reasons=reasons, net_amount=rec.loaded_net_amount,
              extra_columns=hm.extras, ms=round(1000 * (time.perf_counter() - t0)))

    def _quarantined(self, f: LandingFile, rec: FileRecord) -> None:
        self.stats.files_quarantined += 1
        self.stats.rows_rejected += rec.rows_rejected
        self.stats.statuses["QUARANTINED"] += 1
        event(self.log, ERROR, "FILE_QUARANTINED", file=f.file_name, store=f.store_code,
              business_date=rec.business_date, reason=rec.reason_code, rows_rejected=rec.rows_rejected,
              message=rec.message)
