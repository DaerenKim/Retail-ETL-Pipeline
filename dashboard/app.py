"""Aangan Retail - sales & data-quality dashboard.

Reads the dbt marts (warehouse/analytics.duckdb) and the ETL reports (reports/*.csv, run_summary.json).
Run from the repository root after `python -m etl run` and `dbt build ...`:

    streamlit run dashboard/app.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
DB = Path(os.environ.get("RETAIL_ANALYTICS_DB", ROOT / "warehouse" / "analytics.duckdb"))
REPORTS = Path(os.environ.get("RETAIL_REPORT_DIR", ROOT / "reports"))

st.set_page_config(page_title="Aangan Retail - Sales & DQ", layout="wide")


@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    with duckdb.connect(str(DB), read_only=True) as con:
        return con.sql(sql).df()


def usd_m(v: float) -> str:
    return f"${v / 1e6:,.2f}M"


if not DB.exists():
    st.error(f"Analytics database not found at {DB}. Run the pipeline and `dbt build` first (see README).")
    st.stop()

st.title("Aangan Retail · Sales & Data Quality")

stores = q("SELECT store_code, store_name, format, zone FROM stg_stores ORDER BY store_code")
with st.sidebar:
    st.header("Filters")
    formats = st.multiselect("Store format", sorted(stores["format"].unique()), default=sorted(stores["format"].unique()))
    zones = st.multiselect("Zone", sorted(stores["zone"].unique()), default=sorted(stores["zone"].unique()))
    selected = stores[stores["format"].isin(formats) & stores["zone"].isin(zones)]["store_code"].tolist()

if not selected:
    st.warning("No stores match the filters.")
    st.stop()
in_list = ", ".join(f"'{s}'" for s in selected)

tab_sales, tab_stores, tab_dq = st.tabs(["Sales", "Stores", "Data quality"])

# ------------------------------------------------------------------ Sales
with tab_sales:
    daily = q(f"""SELECT business_date, sum(net_amount) AS net_amount, sum(sales_bills) AS bills
                  FROM fct_daily_store_sales WHERE store_code IN ({in_list}) GROUP BY 1 ORDER BY 1""")
    k = q(f"""SELECT sum(net_amount) net, sum(sales_bills) bills, sum(sales_amount) / sum(sales_bills) basket,
                     -100 * sum(returns_amount) / sum(sales_amount) ret
              FROM fct_daily_store_sales WHERE store_code IN ({in_list})""").iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Net sales", usd_m(k["net"]))
    c2.metric("Sales bills", f"{int(k['bills']):,}")
    c3.metric("Avg basket", f"${k['basket']:,.2f}")
    c4.metric("Return rate", f"{k['ret']:.2f}%")

    st.subheader("Daily net sales")
    st.altair_chart(
        alt.Chart(daily).mark_line(point=True).encode(
            x=alt.X("business_date:T", title=None),
            y=alt.Y("net_amount:Q", title="Net sales (USD)"),
            tooltip=[alt.Tooltip("business_date:T"), alt.Tooltip("net_amount:Q", format="$,.0f"),
                     alt.Tooltip("bills:Q", format=",")]),
        use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Sales by category")
        cat = q(f"""SELECT p.category, sum(s.net_amount) AS net_amount
                    FROM stg_sales s JOIN stg_products p USING (product_code)
                    WHERE s.store_code IN ({in_list}) GROUP BY 1 ORDER BY 2 DESC""")
        st.altair_chart(alt.Chart(cat).mark_bar().encode(
            x=alt.X("net_amount:Q", title="Net sales (USD)"), y=alt.Y("category:N", sort="-x", title=None),
            tooltip=["category", alt.Tooltip("net_amount:Q", format="$,.0f")]), use_container_width=True)
    with right:
        st.subheader("Average store-day by weekday")
        dow = q(f"""SELECT day_name, isodow(business_date) AS d, avg(net_amount) AS avg_net
                    FROM fct_daily_store_sales WHERE store_code IN ({in_list}) GROUP BY 1, 2 ORDER BY 2""")
        st.altair_chart(alt.Chart(dow).mark_bar().encode(
            x=alt.X("day_name:N", sort=dow["day_name"].tolist(), title=None),
            y=alt.Y("avg_net:Q", title="Avg net per store-day (USD)"),
            tooltip=["day_name", alt.Tooltip("avg_net:Q", format="$,.0f")]), use_container_width=True)

    st.subheader("Payment mix by format")
    pm = q("SELECT format, payment_mode, share_of_bills_pct FROM mart_payment_mix")
    pm = pm[pm["format"].isin(formats)]
    st.altair_chart(alt.Chart(pm).mark_bar().encode(
        x=alt.X("share_of_bills_pct:Q", stack="normalize", title="Share of bills"),
        y=alt.Y("format:N", title=None), color=alt.Color("payment_mode:N", title="Payment"),
        tooltip=["format", "payment_mode", "share_of_bills_pct"]), use_container_width=True)

# ------------------------------------------------------------------ Stores
with tab_stores:
    sc = q(f"""SELECT store_code, store_name, format, zone, net_sales, avg_daily_net, avg_basket_value,
                      return_rate_pct, discount_rate_pct, net_sales_per_sqft, rank_in_format, vs_format_avg_pct,
                      trading_days
               FROM mart_store_scorecard WHERE store_code IN ({in_list}) ORDER BY net_sales DESC""")
    st.subheader("Store scorecard")
    st.dataframe(sc, use_container_width=True, hide_index=True)
    st.subheader("Top 15 products")
    st.dataframe(q("SELECT sales_rank, product_code, product_name, category, net_units, net_sales, stores_selling "
                   "FROM mart_top_products ORDER BY sales_rank LIMIT 15"), use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ Data quality
with tab_dq:
    summary_path = REPORTS / "run_summary.json"
    if not summary_path.exists():
        st.info("Run `python -m etl run` to produce reports/.")
    else:
        rs = json.loads(summary_path.read_text())
        rec = rs["reconciliation"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Rows read", f"{rs['rows_read']:,}")
        c2.metric("Reject rate", f"{rs['reject_pct']}%", help="Target: below 2%")
        c3.metric("Reconciled store-days", f"{rec['partitions_within_tolerance']:,}/{rec['partitions']:,}",
                  help=f"Within {rec['tolerance_pct']}%")
        full = rs.get("last_full_run") or {}
        c4.metric("Last full rerun", f"{full.get('duration_s') or 0:.0f} s", help="Target: under 600 s")

        left, right = st.columns(2)
        with left:
            st.subheader("Rejected rows by reason")
            dq = pd.read_csv(REPORTS / "dq_summary.csv")
            st.altair_chart(alt.Chart(dq).mark_bar().encode(
                x=alt.X("rejected_rows:Q", title="Rows"), y=alt.Y("reason_code:N", sort="-x", title=None),
                tooltip=["reason_code", "rejected_rows", "pct_of_rows_read"]), use_container_width=True)
        with right:
            st.subheader("Files by status")
            st.dataframe(pd.read_csv(REPORTS / "file_status.csv"), use_container_width=True, hide_index=True)

        st.subheader("Reject rate by store")
        by_store = pd.read_csv(REPORTS / "dq_by_store.csv")
        st.altair_chart(alt.Chart(by_store).mark_bar().encode(
            x=alt.X("store_code:N", title=None), y=alt.Y("reject_pct:Q", title="Reject %"),
            tooltip=["store_code", "rows_read", "rows_rejected", "reject_pct"]), use_container_width=True)

        st.subheader("Missing-store alerts")
        alerts = pd.read_csv(REPORTS / "alerts.csv")
        st.dataframe(alerts, use_container_width=True, hide_index=True) if len(alerts) else st.success("No alerts.")
