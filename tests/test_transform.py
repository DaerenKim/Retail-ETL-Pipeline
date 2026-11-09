"""Unit tests for the standardisation functions in etl/transform.py.
Each SQL expression is executed by DuckDB on hand-written raw values."""
from datetime import datetime
from decimal import Decimal

from etl import transform as T


def test_clean_text_trims_and_nulls_tokens(sql_eval, settings):
    out = sql_eval(lambda c: T.clean_text(c, settings.null_tokens), ["  abc ", "", "NULL", "na", "N/A", None, "0"])
    assert out == ["abc", None, None, None, None, None, "0"]


def test_parse_money_handles_thousands_separator_and_garbage(sql_eval, settings):
    out = sql_eval(lambda c: T.parse_money(c, settings.null_tokens),
                   ["1,234.50", " 84.6 ", "-12.00", "0", "abc", "", "NA", "12,34,567.89"])
    assert out == [Decimal("1234.50"), Decimal("84.60"), Decimal("-12.00"), Decimal("0.00"), None, None, None,
                   Decimal("1234567.89")]


def test_parse_number_accepts_decimal_quantities(sql_eval, settings):
    out = sql_eval(lambda c: T.parse_number(c, settings.null_tokens), ["2", "2.0", "-1.0", "1.5", "x", None])
    assert out == [2.0, 2.0, -1.0, 1.5, None, None]


def test_parse_datetime_every_layout_format(sql_eval, settings):
    fn = lambda c: T.parse_datetime(c, settings.date_formats, settings.null_tokens)  # noqa: E731
    out = sql_eval(fn, ["2026-09-01 14:32:10",      # A
                        "01/09/2026 14:32",         # B (DD/MM)
                        "01-Sep-2026 02:32 PM",     # C
                        "2026-09-01T14:32:10",      # D
                        "01-09-2026 14:32"])        # mixed-in legacy format
    assert out == [datetime(2026, 9, 1, 14, 32, 10), datetime(2026, 9, 1, 14, 32), datetime(2026, 9, 1, 14, 32),
                   datetime(2026, 9, 1, 14, 32, 10), datetime(2026, 9, 1, 14, 32)]


def test_parse_datetime_slash_dates_are_day_first(sql_eval, settings):
    fn = lambda c: T.parse_datetime(c, settings.date_formats, settings.null_tokens)  # noqa: E731
    assert sql_eval(fn, ["03/04/2026 09:00"]) == [datetime(2026, 4, 3, 9, 0)]   # 3 April, not 4 March


def test_parse_datetime_invalid_values_become_null(sql_eval, settings):
    fn = lambda c: T.parse_datetime(c, settings.date_formats, settings.null_tokens)  # noqa: E731
    assert sql_eval(fn, ["31-02-2026 11:15", "2026-13-45 10:00:00", "##########", "0000-00-00 00:00:00", ""]) \
        == [None] * 5


def test_normalise_store_code(sql_eval, settings):
    fn = lambda c: T.normalise_store_code(c, "GGN", 3, settings.null_tokens)  # noqa: E731
    out = sql_eval(fn, [" ggn007", "GGN007 ", "7", "007", "12", "GGN 012", "GGN0O7", "GN007", None])
    assert out == ["GGN007", "GGN007", "GGN007", "GGN007", "GGN012", "GGN012", "GGN0O7", "GN007", None]


def test_normalise_payment(sql_eval, settings):
    fn = lambda c: T.normalise_payment(c, settings.payment_mode_map, settings.null_tokens)  # noqa: E731
    out = sql_eval(fn, ["CASH", "Cash", "upi", "credit_card", "debit_card", "Card", "E-Wallet", "wallet",
                        "bitcoin", None])
    assert out == ["CASH", "CASH", "UPI", "CARD", "CARD", "CARD", "WALLET", "WALLET", "UNKNOWN", "UNKNOWN"]


def test_sql_literal_escapes_quotes():
    assert T.sql_literal("O'Brien") == "'O''Brien'"
    assert T.quote_ident('a"b') == '"a""b"'
