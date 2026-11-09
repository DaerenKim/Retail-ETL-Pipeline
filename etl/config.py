"""Load and validate the pipeline configuration (config/pipeline.yaml + config/schema_contract.yaml)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"


class ConfigError(ValueError):
    """Raised when the configuration file is missing keys or has invalid values."""


@dataclass
class FilePattern:
    name: str
    regex: re.Pattern
    date_format: str


@dataclass
class Settings:
    root: Path
    landing_dir: Path
    master_dir: Path
    silver_dir: Path
    log_dir: Path
    report_dir: Path
    warehouse_url: str
    store_source: str
    store_list: list[str]
    store_prefix: str
    store_digits: int
    file_patterns: list[FilePattern]
    date_formats: list[str]
    payment_mode_map: dict[str, str]
    null_tokens: list[str]
    thresholds: dict[str, float]
    fuzzy_auto_map: bool
    fuzzy_cutoff: float
    missing_consecutive_days: int
    write_silver: bool
    contract: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def required_columns(self) -> list[str]:
        return [c for c, spec in self.contract.items() if spec.get("required")]


REQUIRED_THRESHOLDS = ["max_quantity", "amount_tolerance", "price_tolerance_pct", "date_tolerance_days",
                       "max_file_reject_pct", "max_run_reject_pct", "reconciliation_tolerance_pct"]


def _require(d: dict, key: str, where: str):
    if key not in d:
        raise ConfigError(f"missing '{key}' in {where}")
    return d[key]


def load_settings(config_path: str | Path | None = None, overrides: dict | None = None) -> Settings:
    """Read the YAML config, resolve paths against the project root and validate it.

    `overrides` is a nested dict merged on top of the file (used by tests to point at temp folders).
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    raw = yaml.safe_load(path.read_text()) or {}
    if overrides:
        raw = _deep_merge(raw, overrides)

    root = path.resolve().parent.parent
    paths = _require(raw, "paths", "config")

    def p(key: str) -> Path:
        val = Path(_require(paths, key, "paths"))
        return val if val.is_absolute() else root / val

    url = os.environ.get("RETAIL_WAREHOUSE_URL") or _require(_require(raw, "warehouse", "config"), "url", "warehouse")
    if url.startswith("sqlite:///"):
        db_path = Path(url[len("sqlite:///"):])
        if not db_path.is_absolute():
            url = f"sqlite:///{root / db_path}"
    elif not url.startswith(("postgresql://", "postgres://")):
        raise ConfigError(f"unsupported warehouse url: {url!r} (use sqlite:/// or postgresql://)")

    patterns = []
    for fp in _require(raw, "file_patterns", "config"):
        try:
            rx = re.compile(fp["regex"])
        except re.error as exc:
            raise ConfigError(f"bad regex in file pattern {fp.get('name')}: {exc}") from exc
        if not {"store", "date"} <= set(rx.groupindex):
            raise ConfigError(f"file pattern {fp.get('name')} needs named groups 'store' and 'date'")
        patterns.append(FilePattern(fp.get("name", "pattern"), rx, fp["date_format"]))

    thresholds = _require(raw, "thresholds", "config")
    for key in REQUIRED_THRESHOLDS:
        _require(thresholds, key, "thresholds")
        if float(thresholds[key]) < 0:
            raise ConfigError(f"threshold {key} must be >= 0")

    stores = raw.get("stores", {})
    if stores.get("source", "master") not in ("master", "list"):
        raise ConfigError("stores.source must be 'master' or 'list'")

    drift = raw.get("schema_drift", {})
    contract_path = p("schema_contract")
    contract = (yaml.safe_load(contract_path.read_text()) or {}).get("columns", {})
    if not contract:
        raise ConfigError(f"schema contract {contract_path} has no columns")

    return Settings(
        root=root,
        landing_dir=p("landing_dir"), master_dir=p("master_dir"), silver_dir=p("silver_dir"),
        log_dir=p("log_dir"), report_dir=p("report_dir"),
        warehouse_url=url,
        store_source=stores.get("source", "master"),
        store_list=[s.upper() for s in stores.get("list", [])],
        store_prefix=stores.get("code_prefix", "GGN"),
        store_digits=int(stores.get("code_digits", 3)),
        file_patterns=patterns,
        date_formats=list(_require(raw, "date_formats", "config")),
        payment_mode_map={k.lower(): v for k, v in raw.get("payment_mode_map", {}).items()},
        null_tokens=[t.upper() for t in raw.get("null_tokens", [""])],
        thresholds={k: float(v) for k, v in thresholds.items()},
        fuzzy_auto_map=bool(drift.get("fuzzy_auto_map", True)),
        fuzzy_cutoff=float(drift.get("fuzzy_cutoff", 0.85)),
        missing_consecutive_days=int(raw.get("alerts", {}).get("missing_consecutive_days", 2)),
        write_silver=bool(raw.get("outputs", {}).get("write_silver_parquet", True)),
        contract=contract,
    )


def _deep_merge(base: dict, extra: dict) -> dict:
    out = dict(base)
    for k, v in extra.items():
        out[k] = _deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out
