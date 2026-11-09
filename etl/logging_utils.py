"""Structured logging: every event is one JSON object per line in logs/run_<run_id>.jsonl,
plus a short human-readable line on the console."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

LOGGER_NAME = "retail_etl"


class JsonLinesFormatter(logging.Formatter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "run_id": self.run_id,
            "level": record.levelname,
            "event": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        return json.dumps(payload, default=str)


class ConsoleFormatter(logging.Formatter):
    KEYS = ("file", "store", "business_date", "rows_loaded", "rows_rejected", "reason", "message")

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "fields", {})
        extras = " ".join(f"{k}={fields[k]}" for k in self.KEYS if k in fields)
        return f"{record.levelname:<7} {record.getMessage():<26} {extras}".rstrip()


def setup_logging(run_id: str, log_dir: Path, console_level: int = logging.INFO) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run_{run_id}.jsonl"
    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonLinesFormatter(run_id))
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(ConsoleFormatter())
    logger.addHandler(ch)
    return logger, log_path


def event(logger: logging.Logger, level: int, name: str, **fields) -> None:
    """Log a named event with structured fields, e.g. event(log, INFO, "FILE_LOADED", file=..., rows=...)."""
    logger.log(level, name, extra={"fields": fields})


def close_logging(logger: logging.Logger) -> None:
    for h in list(logger.handlers):
        h.close()
        logger.removeHandler(h)
