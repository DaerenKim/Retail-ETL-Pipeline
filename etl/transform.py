"""Standardisation rules, written as small functions that return DuckDB SQL expressions.

Each function takes the SQL expression of a raw text column and returns the SQL expression of the
cleaned value. Keeping them as tiny pure functions means:
  * the whole file is cleaned in one vectorised DuckDB query (fast: ~2M rows in seconds)
  * every rule can be unit-tested on its own (see tests/test_transform.py, which runs each
    expression through DuckDB on hand-written inputs)

Nothing here rejects a row - it only produces clean values or NULL. Rejection is validate.py's job.
"""
from __future__ import annotations

from .config import Settings
from .schema import HeaderMapping


def sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def clean_text(col: str, null_tokens: list[str]) -> str:
    """Trim whitespace; blank or a null token ('NULL', 'NA', ...) becomes NULL."""
    tokens = ", ".join(sql_literal(t) for t in null_tokens)
    return f"CASE WHEN {col} IS NULL OR upper(trim({col})) IN ({tokens}) THEN NULL ELSE trim({col}) END"


def parse_money(col: str, null_tokens: list[str]) -> str:
    """'1,234.50' -> 1234.50, ' 84.6 ' -> 84.60, 'abc' -> NULL."""
    return f"TRY_CAST(replace({clean_text(col, null_tokens)}, ',', '') AS DECIMAL(14, 2))"


def parse_number(col: str, null_tokens: list[str]) -> str:
    """Quantity as a DOUBLE so that '2.0' and '2' both work; integrality is checked in validation."""
    return f"TRY_CAST(replace({clean_text(col, null_tokens)}, ',', '') AS DOUBLE)"


def parse_datetime(col: str, formats: list[str], null_tokens: list[str]) -> str:
    """Try each configured format in order; the first that parses wins. Unparseable -> NULL."""
    cleaned = clean_text(col, null_tokens)
    attempts = ", ".join(f"try_strptime({cleaned}, {sql_literal(f)})" for f in formats)
    return f"COALESCE({attempts})"


def normalise_store_code(col: str, prefix: str, digits: int, null_tokens: list[str]) -> str:
    """' ggn007' -> 'GGN007', 'GGN007 ' -> 'GGN007', legacy numeric '7' / '007' -> 'GGN007'.
    Anything else is upper-cased and left for validation to judge (e.g. 'GGN0O7' stays as is)."""
    s = f"upper(replace({clean_text(col, null_tokens)}, ' ', ''))"
    return (f"CASE WHEN regexp_full_match({s}, '[0-9]{{1,{digits}}}') "
            f"THEN {sql_literal(prefix)} || lpad({s}, {digits}, '0') ELSE {s} END")


def normalise_payment(col: str, mapping: dict[str, str], null_tokens: list[str]) -> str:
    """'Cash' -> 'CASH', 'credit_card' -> 'CARD', 'E-Wallet' -> 'WALLET'; unknown -> 'UNKNOWN'."""
    s = f"lower({clean_text(col, null_tokens)})"
    whens = " ".join(f"WHEN {sql_literal(k)} THEN {sql_literal(v)}" for k, v in mapping.items())
    return f"CASE {s} {whens} ELSE 'UNKNOWN' END"


def standardise_select(mapping: HeaderMapping, settings: Settings, raw_table: str = "raw") -> str:
    """Build the SELECT that turns raw text columns (c0, c1, ...) into typed canonical columns.

    Missing optional columns get their contract default. Raw text is kept alongside the parsed
    values so that validation can tell 'blank' apart from 'present but invalid'.
    """
    nt = settings.null_tokens

    def raw(canonical: str) -> str:
        idx = mapping.mapping.get(canonical)
        return f"c{idx}" if idx is not None else "CAST(NULL AS VARCHAR)"

    all_cols = [f"c{i}" for i in range(len(mapping.raw_headers))]
    json_pairs = ", ".join(f"{sql_literal(h)}, c{i}" for i, h in enumerate(mapping.raw_headers))
    has_discount = "discount" in mapping.mapping
    has_payment = "payment_mode" in mapping.mapping

    return f"""
SELECT
    row_number,
    {clean_text(raw('bill_no'), nt)}                                   AS bill_no,
    {clean_text(raw('bill_datetime'), nt)}                             AS bill_datetime_raw,
    {parse_datetime(raw('bill_datetime'), settings.date_formats, nt)}  AS bill_ts,
    {clean_text(raw('store_code'), nt)}                                AS store_code_raw,
    {normalise_store_code(raw('store_code'), settings.store_prefix, settings.store_digits, nt)} AS store_code,
    upper({clean_text(raw('product_code'), nt)})                       AS product_code,
    {clean_text(raw('quantity'), nt)}                                  AS quantity_raw,
    {parse_number(raw('quantity'), nt)}                                AS quantity_num,
    {clean_text(raw('unit_price'), nt)}                                AS unit_price_raw,
    {parse_money(raw('unit_price'), nt)}                               AS unit_price,
    {clean_text(raw('discount'), nt)}                                  AS discount_raw,
    {"COALESCE(" + parse_money(raw('discount'), nt) + ", CASE WHEN " + clean_text(raw('discount'), nt)
      + " IS NULL THEN 0 END)" if has_discount else "CAST(0 AS DECIMAL(14, 2))"}  AS discount,
    {clean_text(raw('net_amount'), nt)}                                AS net_amount_raw,
    {parse_money(raw('net_amount'), nt)}                               AS net_amount,
    {normalise_payment(raw('payment_mode'), settings.payment_mode_map, nt) if has_payment else "'UNKNOWN'"} AS payment_mode,
    md5(concat_ws(chr(31), {", ".join(f"coalesce({c}, '')" for c in all_cols)}))  AS raw_hash,
    CAST(json_object({json_pairs}) AS VARCHAR)                         AS raw_record
FROM {raw_table}
"""
