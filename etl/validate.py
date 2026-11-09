"""Data quality rules. Every row gets at most one reason code: the FIRST rule it fails, in the order
below. Rows with no reason code are loaded; the rest go to etl_rejected_rows.

 order  reason_code              rule
 -----  -----------------------  ---------------------------------------------------------------
   1    DUPLICATE_ROW            exact copy (every raw column identical) of an earlier row
   2    MISSING_REQUIRED_FIELD   a required field is blank / a null token
   3    INVALID_DATE             timestamp unparseable, or not on the file's business date
   4    INVALID_NUMBER           quantity / price / discount / amount not numeric, or qty fractional
   5    UNKNOWN_STORE            store not in dim_store, or not the store named in the filename
   6    UNKNOWN_PRODUCT          product not in dim_product
   7    INVALID_QUANTITY         quantity = 0 or |quantity| > max_quantity
   8    INVALID_PRICE            unit_price <= 0 or above MRP (+ tolerance)
   9    AMOUNT_MISMATCH          negative discount, |qty*price - discount - net| > tolerance ($0.05),
                                 or net and quantity have opposite signs
  10    DUPLICATE_ROW            same (store, bill_no, product_code) as an earlier VALID row

Returns (negative quantity with negative net amount) pass every rule: they are real sales events.
"""
from __future__ import annotations

from datetime import date

import duckdb

from .config import Settings
from .transform import sql_literal

REASON_CODES = [
    "DUPLICATE_ROW", "MISSING_REQUIRED_FIELD", "INVALID_DATE", "INVALID_NUMBER", "UNKNOWN_STORE",
    "UNKNOWN_PRODUCT", "INVALID_QUANTITY", "INVALID_PRICE", "AMOUNT_MISMATCH",
]

FILE_REASON_CODES = [
    "DUPLICATE_FILE", "EMPTY_FILE", "ZERO_BYTE_FILE", "UNPARSEABLE_FILENAME", "UNREADABLE_FILE",
    "SCHEMA_MISSING_COLUMN", "UNKNOWN_STORE_FILE", "STALE_VERSION", "REJECT_RATE_EXCEEDED",
]


def rule_case_sql(settings: Settings, file_store: str, business_date: date) -> str:
    """The CASE expression implementing rules 1-9 (rule 10 needs the result of 1-9)."""
    t = settings.thresholds
    price_factor = 1 + t["price_tolerance_pct"] / 100
    return f"""
    CASE
      WHEN s.exact_copy > 1 THEN 'DUPLICATE_ROW'
      WHEN s.bill_no IS NULL OR s.bill_datetime_raw IS NULL OR s.store_code_raw IS NULL
        OR s.product_code IS NULL OR s.quantity_raw IS NULL OR s.unit_price_raw IS NULL
        OR s.net_amount_raw IS NULL THEN 'MISSING_REQUIRED_FIELD'
      WHEN s.bill_ts IS NULL
        OR abs(date_diff('day', CAST(s.bill_ts AS DATE), DATE {sql_literal(business_date.isoformat())}))
           > {int(t['date_tolerance_days'])} THEN 'INVALID_DATE'
      WHEN s.quantity_num IS NULL OR s.unit_price IS NULL OR s.discount IS NULL OR s.net_amount IS NULL
        OR s.quantity_num <> trunc(s.quantity_num) THEN 'INVALID_NUMBER'
      WHEN st.store_code IS NULL OR s.store_code <> {sql_literal(file_store)} THEN 'UNKNOWN_STORE'
      WHEN p.product_code IS NULL THEN 'UNKNOWN_PRODUCT'
      WHEN s.quantity_num = 0 OR abs(s.quantity_num) > {t['max_quantity']} THEN 'INVALID_QUANTITY'
      WHEN s.unit_price <= 0 OR s.unit_price > p.mrp * {price_factor} THEN 'INVALID_PRICE'
      WHEN s.discount < 0
        OR abs(s.quantity_num * s.unit_price - s.discount - s.net_amount) > {t['amount_tolerance']}
        OR (s.net_amount <> 0 AND sign(s.net_amount) <> sign(s.quantity_num)) THEN 'AMOUNT_MISMATCH'
    END"""


DETAIL_SQL = """
    CASE reason_code
      WHEN 'DUPLICATE_ROW' THEN 'duplicate of an earlier row (bill ' || coalesce(bill_no, '?') || ')'
      WHEN 'MISSING_REQUIRED_FIELD' THEN 'blank: ' || concat_ws(', ',
            CASE WHEN bill_no IS NULL THEN 'bill_no' END,
            CASE WHEN bill_datetime_raw IS NULL THEN 'bill_datetime' END,
            CASE WHEN store_code_raw IS NULL THEN 'store_code' END,
            CASE WHEN product_code IS NULL THEN 'product_code' END,
            CASE WHEN quantity_raw IS NULL THEN 'quantity' END,
            CASE WHEN unit_price_raw IS NULL THEN 'unit_price' END,
            CASE WHEN net_amount_raw IS NULL THEN 'net_amount' END)
      WHEN 'INVALID_DATE' THEN 'bill_datetime=' || bill_datetime_raw
      WHEN 'INVALID_NUMBER' THEN 'qty=' || coalesce(quantity_raw, '') || ' price=' || coalesce(unit_price_raw, '')
            || ' discount=' || coalesce(discount_raw, '') || ' net=' || coalesce(net_amount_raw, '')
      WHEN 'UNKNOWN_STORE' THEN 'store_code=' || coalesce(store_code_raw, '')
      WHEN 'UNKNOWN_PRODUCT' THEN 'product_code=' || product_code
      WHEN 'INVALID_QUANTITY' THEN 'quantity=' || quantity_raw
      WHEN 'INVALID_PRICE' THEN 'unit_price=' || unit_price_raw || ' mrp=' || coalesce(CAST(mrp AS VARCHAR), '?')
      WHEN 'AMOUNT_MISMATCH' THEN 'net=' || net_amount_raw || ' expected='
            || CAST(round(quantity_num * unit_price - discount, 2) AS VARCHAR)
    END"""


def validate(con: duckdb.DuckDBPyConnection, settings: Settings, file_store: str, business_date: date,
             std_table: str = "std", out_table: str = "validated") -> None:
    """Create TEMP table `out_table` = std columns + mrp + reason_code + reason_detail."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _pre AS
        WITH s AS (
            SELECT *, row_number() OVER (PARTITION BY raw_hash ORDER BY row_number) AS exact_copy
            FROM {std_table}
        )
        SELECT s.* EXCLUDE (exact_copy), p.mrp,
               {rule_case_sql(settings, file_store, business_date)} AS reason_code
        FROM s
        LEFT JOIN dim_store st ON st.store_code = s.store_code
        LEFT JOIN dim_product p ON p.product_code = s.product_code
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out_table} AS
        WITH k AS (
            SELECT *,
                   CASE WHEN reason_code IS NULL THEN
                        row_number() OVER (PARTITION BY reason_code IS NULL, store_code, bill_no, product_code
                                           ORDER BY row_number) END AS key_copy
            FROM _pre
        ),
        r AS (
            SELECT * EXCLUDE (reason_code, key_copy),
                   CASE WHEN reason_code IS NULL AND key_copy > 1 THEN 'DUPLICATE_ROW' ELSE reason_code END
                       AS reason_code
            FROM k
        )
        SELECT *, {DETAIL_SQL} AS reason_detail FROM r
        ORDER BY row_number
    """)
