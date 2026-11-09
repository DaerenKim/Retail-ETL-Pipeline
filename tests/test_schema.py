"""Header mapping and schema-drift detection (etl/schema.py)."""
import pytest

from etl.schema import layout_signature, map_headers, normalise_header

LAYOUTS = {
    "A": "bill_no,bill_date,store_code,product_code,qty,unit_price,discount,net_amount,payment_mode",
    "B": "BillDate,BillNo,StoreID,ItemCode,ItemName,Qty,Rate,Disc,Amount,PayMode",
    "C": "invoice_id,txn_ts,outlet_code,terminal_id,sku,units,selling_price,discount_value,line_total,"
         "tender_type,customer_id,loyalty_points",
    "D": "bill_number,bill_date,store_code,product_code,sale_qty,unit_price,discount,gst_amount,net_amount,"
         "payment_mode,terminal_id",
}
EXTRAS = {"A": [], "B": ["ItemName"], "C": ["terminal_id", "customer_id", "loyalty_points"],
          "D": ["gst_amount", "terminal_id"]}


@pytest.mark.parametrize("layout", list(LAYOUTS))
def test_every_layout_maps_all_contract_columns(layout, settings):
    headers = LAYOUTS[layout].split(",")
    hm = map_headers(headers, settings.contract)
    assert hm.ok
    assert set(hm.mapping) == set(settings.contract)       # all 9 canonical columns found
    assert hm.extras == EXTRAS[layout]
    assert not hm.auto_mapped


def test_mapping_points_at_the_right_source_columns(settings):
    hm = map_headers(LAYOUTS["B"].split(","), settings.contract)
    assert hm.source_header("bill_datetime") == "BillDate"
    assert hm.source_header("net_amount") == "Amount"
    assert hm.source_header("store_code") == "StoreID"


@pytest.mark.parametrize("raw,expected", [
    ("﻿BillNo", "billno"), ("  BILL_NO ", "bill_no"), ("Sale Qty", "sale_qty"),
    ("line-total", "line_total"), (" QTY ", "qty"), ("net__amount", "net_amount"),
])
def test_normalise_header(raw, expected):
    assert normalise_header(raw) == expected


def test_noisy_uppercase_headers_still_map(settings):
    noisy = ["BILL_NO ", " BILL_DATE", "STORE_CODE ", "PRODUCT_CODE", " QTY ", "UNIT_PRICE", "DISCOUNT ",
             " NET_AMOUNT", "PAYMENT_MODE "]
    hm = map_headers(noisy, settings.contract)
    assert hm.ok and not hm.extras


def test_missing_required_column_is_reported(settings):
    headers = [h for h in LAYOUTS["A"].split(",") if h != "net_amount"]
    hm = map_headers(headers, settings.contract)
    assert not hm.ok
    assert hm.missing_required == ["net_amount"]


def test_missing_optional_column_is_fine(settings):
    headers = [h for h in LAYOUTS["A"].split(",") if h not in ("discount", "payment_mode")]
    assert map_headers(headers, settings.contract).ok


def test_fuzzy_auto_map_for_renamed_column(settings):
    headers = LAYOUTS["A"].replace("net_amount", "net_amnt").split(",")
    hm = map_headers(headers, settings.contract, fuzzy=True, cutoff=0.85)
    assert hm.ok
    assert hm.auto_mapped == {"net_amount": "net_amnt"}


def test_fuzzy_auto_map_respects_cutoff_and_switch(settings):
    headers = LAYOUTS["A"].replace("net_amount", "total_value_inr").split(",")
    assert not map_headers(headers, settings.contract, fuzzy=True, cutoff=0.85).ok
    headers = LAYOUTS["A"].replace("net_amount", "net_amnt").split(",")
    assert not map_headers(headers, settings.contract, fuzzy=False).ok


def test_layout_signature_ignores_order_and_case_but_detects_drift():
    a = LAYOUTS["A"].split(",")
    assert layout_signature(a) == layout_signature([h.upper() for h in reversed(a)])
    assert layout_signature(a) != layout_signature(LAYOUTS["D"].split(","))
