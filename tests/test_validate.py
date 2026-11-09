"""One test per data-quality rule (etl/validate.py), plus rule priority and the 'returns are valid' case."""
import pytest

from tests.conftest import A_HEADER, a_row


def reasons(rows):
    return [r["reason_code"] for r in rows]


def test_clean_row_passes(run_rows):
    rows = run_rows(A_HEADER, [a_row()])
    assert reasons(rows) == [None]
    assert rows[0]["store_code"] == "GGN001" and rows[0]["payment_mode"] == "CASH"


def test_return_line_is_valid(run_rows):
    rows = run_rows(A_HEADER, [a_row(bill_no="RB1", qty="-1", net_amount="-90.00")])
    assert reasons(rows) == [None]


def test_benign_variations_are_loaded(run_rows):
    rows = run_rows(A_HEADER, [
        a_row(bill_no="B1", store_code=" ggn001", payment_mode="Cash"),
        a_row(bill_no="B2", bill_date="01-09-2026 10:00"),        # mixed date format
        a_row(bill_no="B3", store_code="1"),                     # legacy numeric store id
        a_row(bill_no="B4", qty="2.0"),
    ])
    assert reasons(rows) == [None, None, None, None]


@pytest.mark.parametrize("field", ["bill_no", "bill_date", "store_code", "product_code", "qty", "unit_price",
                                   "net_amount"])
@pytest.mark.parametrize("blank", ["", "NULL", "NA"])
def test_missing_required_field(run_rows, field, blank):
    rows = run_rows(A_HEADER, [a_row(**{field: blank})])
    assert reasons(rows) == ["MISSING_REQUIRED_FIELD"]


def test_blank_discount_is_zero_not_an_error(run_rows):
    rows = run_rows(A_HEADER, [a_row(discount="")])
    assert reasons(rows) == [None] and rows[0]["discount"] == 0


@pytest.mark.parametrize("value", ["31-02-2026 11:15", "##########", "2062-09-01 10:00:00", "2026-09-02 10:00:00"])
def test_invalid_or_wrong_day_date(run_rows, value):
    assert reasons(run_rows(A_HEADER, [a_row(bill_date=value)])) == ["INVALID_DATE"]


@pytest.mark.parametrize("kw", [{"qty": "two"}, {"qty": "1.5", "net_amount": "135.00"}, {"unit_price": "9O.00"},
                                {"net_amount": "1.8e"}, {"discount": "abc"}])
def test_invalid_number(run_rows, kw):
    assert reasons(run_rows(A_HEADER, [a_row(**kw)])) == ["INVALID_NUMBER"]


@pytest.mark.parametrize("store", ["GGN099", "GGN0O1", "GN001", "GGN007"])
def test_unknown_store_or_store_not_matching_filename(run_rows, store):
    # GGN007 exists in the master, but the file belongs to GGN001
    assert reasons(run_rows(A_HEADER, [a_row(store_code=store)])) == ["UNKNOWN_STORE"]


def test_unknown_product(run_rows):
    assert reasons(run_rows(A_HEADER, [a_row(product_code="SKU91234")])) == ["UNKNOWN_PRODUCT"]


@pytest.mark.parametrize("qty", ["0", "9999", "-999", "501"])
def test_invalid_quantity(run_rows, qty):
    assert reasons(run_rows(A_HEADER, [a_row(qty=qty)])) == ["INVALID_QUANTITY"]


@pytest.mark.parametrize("price", ["0", "-90.00", "150.00", "9000.00"])
def test_invalid_price_including_above_mrp(run_rows, price):
    assert reasons(run_rows(A_HEADER, [a_row(unit_price=price)])) == ["INVALID_PRICE"]


def test_price_within_mrp_tolerance_is_ok(run_rows):
    # MRP 100, tolerance 1% -> 100.99 accepted
    assert reasons(run_rows(A_HEADER, [a_row(unit_price="100.99", net_amount="201.98")])) == [None]


@pytest.mark.parametrize("kw", [{"net_amount": "500.00"}, {"net_amount": "-180.00"},
                                {"discount": "-5.00", "net_amount": "185.00"},
                                {"qty": "-1", "net_amount": "90.00"}])
def test_amount_mismatch(run_rows, kw):
    assert reasons(run_rows(A_HEADER, [a_row(**kw)])) == ["AMOUNT_MISMATCH"]


def test_amount_within_tolerance_is_ok(run_rows):
    # tolerance is $0.05: 180.04 passes, 180.10 does not
    assert reasons(run_rows(A_HEADER, [a_row(net_amount="180.04")])) == [None]
    assert reasons(run_rows(A_HEADER, [a_row(bill_no="B2", net_amount="180.10")])) == ["AMOUNT_MISMATCH"]


def test_exact_duplicate_rows(run_rows):
    rows = run_rows(A_HEADER, [a_row(), a_row(bill_no="B2"), a_row()])
    assert reasons(rows) == [None, None, "DUPLICATE_ROW"]


def test_duplicate_natural_key_with_different_content(run_rows):
    rows = run_rows(A_HEADER, [a_row(), a_row(payment_mode="UPI")])
    assert reasons(rows) == [None, "DUPLICATE_ROW"]


def test_copy_of_a_bad_row_is_a_duplicate_and_bad_original_does_not_block_a_valid_row(run_rows):
    bad = a_row(qty="9999")
    rows = run_rows(A_HEADER, [bad, bad, a_row(qty="2")])
    # third row shares the key with the (invalid) first row but is itself valid -> loaded
    assert reasons(rows) == ["INVALID_QUANTITY", "DUPLICATE_ROW", None]


def test_rule_priority_first_failure_wins(run_rows):
    # unknown product AND bad quantity AND amount mismatch -> UNKNOWN_PRODUCT (rule 6 before 7, 9)
    rows = run_rows(A_HEADER, [a_row(product_code="SKU99999", qty="0", net_amount="5")])
    assert reasons(rows) == ["UNKNOWN_PRODUCT"]


def test_reason_detail_is_informative(run_rows):
    rows = run_rows(A_HEADER, [a_row(qty="9999"), a_row(bill_no="B2", product_code="")])
    assert rows[0]["reason_detail"] == "quantity=9999"
    assert rows[1]["reason_detail"] == "blank: product_code"


def test_raw_record_keeps_original_values(run_rows):
    import json

    rows = run_rows(A_HEADER, [a_row(store_code=" ggn001")])
    raw = json.loads(rows[0]["raw_record"])
    assert raw["store_code"] == " ggn001" and raw["qty"] == "2"
