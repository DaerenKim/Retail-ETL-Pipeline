"""Smoke test: the dashboard renders without exceptions against the real analytics DB (if built)."""
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "warehouse" / "analytics.duckdb"

streamlit_testing = pytest.importorskip("streamlit.testing.v1")


@pytest.mark.skipif(not DB.exists(), reason="run the pipeline and dbt build first")
def test_dashboard_renders():
    at = streamlit_testing.AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=60)
    at.run()
    assert not at.exception, at.exception
    assert len(at.metric) >= 8
    assert at.title[0].value.startswith("Aangan Retail")
