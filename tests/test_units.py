"""Unit tests for filename parsing, config validation and the missing-day alert logic."""
from datetime import date

import pytest

from etl.alerts import find_missing_runs
from etl.config import ConfigError, load_settings
from etl.discover import discover_files, parse_filename
from etl.fingerprint import EMPTY_SHA256, sha256_file


@pytest.mark.parametrize("name,expected", [
    ("GGN005_20260901.csv", ("GGN005", date(2026, 9, 1), 1, "pos_v3")),
    ("GGN017_20260923_v2.csv", ("GGN017", date(2026, 9, 23), 2, "pos_v3")),
    ("GGN017_20260923_resend.csv", ("GGN017", date(2026, 9, 23), 1, "pos_v3")),
    ("GGN017_20260923 (1).csv", ("GGN017", date(2026, 9, 23), 1, "pos_v3")),
    ("sales_GGN014_29-09-2026.csv", ("GGN014", date(2026, 9, 29), 1, "legacy_pos")),
    ("sales_GGN030_04-09-2026_v2.csv", ("GGN030", date(2026, 9, 4), 2, "legacy_pos")),
    ("GGN001-2026-09-01-export.csv", ("GGN001", date(2026, 9, 1), 1, "cloud_pos")),
    ("ggn001-2026-09-01-export.csv", ("GGN001", date(2026, 9, 1), 1, "cloud_pos")),
])
def test_parse_filename(settings, name, expected):
    assert parse_filename(name, settings.file_patterns) == expected


@pytest.mark.parametrize("name", ["random.csv", "GGN005_2026-09-01.csv", "GGN005_20261341.csv", "sales_GGN1_01-09-2026.csv"])
def test_unparseable_filenames(settings, name):
    assert parse_filename(name, settings.file_patterns) is None


def test_discover_orders_by_arrival_folder_and_respects_until(tmp_path, settings):
    for folder, name in [("2026-09-03", "GGN001_20260902.csv"), ("2026-09-02", "GGN001_20260901.csv"),
                         ("2026-09-02", "notes.txt")]:
        (tmp_path / folder).mkdir(exist_ok=True)
        (tmp_path / folder / name).write_text("x")
    files = discover_files(tmp_path, settings.file_patterns)
    assert [f.file_name for f in files] == ["GGN001_20260901.csv", "GGN001_20260902.csv"]
    assert [f.file_name for f in discover_files(tmp_path, settings.file_patterns, until=date(2026, 9, 2))] \
        == ["GGN001_20260901.csv"]


def test_sha256_identifies_content_not_name(tmp_path):
    a, b, c = tmp_path / "a.csv", tmp_path / "b.csv", tmp_path / "c.csv"
    a.write_text("same")
    b.write_text("same")
    c.write_bytes(b"")
    assert sha256_file(a) == sha256_file(b)
    assert sha256_file(c) == EMPTY_SHA256


def test_config_rejects_bad_values():
    with pytest.raises(ConfigError):
        load_settings(overrides={"warehouse": {"url": "mysql://x"}})
    with pytest.raises(ConfigError):
        load_settings(overrides={"stores": {"source": "everything"}})
    with pytest.raises(ConfigError):
        load_settings(overrides={"thresholds": {"max_quantity": -1}})
    with pytest.raises(ConfigError):
        load_settings(overrides={"file_patterns": [{"name": "x", "regex": "^(?P<shop>.+)$", "date_format": "%Y"}]})
    with pytest.raises(ConfigError):
        load_settings("does/not/exist.yaml")


def test_config_paths_resolve_against_project_root(settings):
    assert settings.landing_dir.is_absolute() and settings.landing_dir.name == "landing"
    assert settings.warehouse_url.startswith("sqlite:////")


def d(day):
    return date(2026, 9, day)


def test_missing_runs_detects_two_or_more_consecutive_days():
    present = {("S1", d(i)) for i in range(1, 11) if i not in (3, 6, 7)} | {("S2", d(i)) for i in range(1, 11)}
    runs = find_missing_runs(present, {"S1": None, "S2": None}, d(1), d(10), min_run=2)
    assert runs == [("S1", d(6), d(7), 2)]


def test_missing_runs_at_window_end_and_open_date():
    present = {("S1", d(i)) for i in range(1, 9)}                # days 9, 10 missing at the end
    present |= {("S2", d(i)) for i in range(5, 11)}              # S2 opened on day 5
    runs = find_missing_runs(present, {"S1": None, "S2": d(5)}, d(1), d(10), min_run=2)
    assert runs == [("S1", d(9), d(10), 2)]


def test_missing_runs_threshold_is_configurable():
    present = {("S1", d(i)) for i in (1, 5)}
    assert find_missing_runs(present, {"S1": None}, d(1), d(5), min_run=3) == [("S1", d(2), d(4), 3)]
    assert find_missing_runs(present, {"S1": None}, d(1), d(5), min_run=4) == []
