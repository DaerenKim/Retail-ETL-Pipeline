#!/usr/bin/env python3
"""
generate_data.py - synthetic daily sales feeds for the Retail ETL Pipeline project.

Simulates "Aangan Retail", a fictional 40-store grocery / general-merchandise chain
in Gurgaon, whose outlets push one CSV per store per business day into a landing
area. The feeds are deliberately messy so the ETL pipeline has real problems to solve:

  * 4 different POS export layouts (different headers, column order, date formats,
    number formats, encodings, line endings, extra columns)
  * schema drift: some stores switch layout part-way through the period
  * late files, files delivered twice, a revised (v2) re-export, an empty file,
    a zero-byte file that is re-sent later, and stores that send nothing
  * a store that is not in the store master (newly opened outlet)
  * row-level defects: nulls, bad quantities, bad prices, amount mismatches,
    unknown product/store codes, unparseable dates and duplicated bills
  * benign variations that must be standardised, NOT rejected
    (mixed date formats, store code casing, payment mode spellings, returns)

Everything is driven by a seed, so the same command always produces byte-identical
output. A ground-truth folder records every injected defect and the expected clean
daily totals so you can prove your pipeline is correct. The pipeline itself must
NOT read the ground-truth folder - it is for tests and reconciliation evidence only.

Uses only the Python standard library.

Usage:
    python generator/generate_data.py                       # full dataset (~2M rows)
    python generator/generate_data.py --stores 5 --days 7 --out data_sample   # tiny sample
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import shutil
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

# --------------------------------------------------------------------------------------
# Static reference data
# --------------------------------------------------------------------------------------
CHAIN_NAME = "Aangan Retail"
START_DATE = date(2026, 9, 1)

LOCALITIES = [
    ("DLF Phase 1", "North"), ("DLF Phase 2", "North"), ("DLF Phase 3", "North"),
    ("DLF Phase 4", "Central"), ("DLF Phase 5", "Central"), ("MG Road", "North"),
    ("Sector 14", "West"), ("Sector 15", "West"), ("Sector 17", "West"),
    ("Sector 23", "West"), ("Sector 29", "Central"), ("Sector 31", "Central"),
    ("Sector 45", "Central"), ("Sector 46", "Central"), ("Sector 56", "East"),
    ("Sector 57", "East"), ("Golf Course Road", "East"), ("Golf Course Extension", "East"),
    ("Sohna Road", "South"), ("South City 1", "Central"), ("South City 2", "South"),
    ("Sushant Lok 1", "Central"), ("Nirvana Country", "South"), ("Palam Vihar", "West"),
    ("Udyog Vihar", "North"), ("Cyber City", "North"), ("Sector 82", "South"),
    ("Sector 83", "South"), ("New Gurgaon", "South"), ("Manesar", "South"),
    ("Sector 49", "East"), ("Sector 50", "East"), ("Sector 67", "East"),
    ("Sector 70", "South"), ("Sector 4", "West"), ("Sector 9", "West"),
    ("Sector 10A", "West"), ("Ardee City", "Central"), ("Malibu Towne", "Central"),
    ("Badshahpur", "South"),
]
UNKNOWN_STORE_LOCALITY = ("Sector 102", "West")

# category -> (items, pack sizes, (min MRP, max MRP) in USD, GST %)
CATALOG = {
    "Staples": (["Basmati Rice", "Sona Masoori Rice", "Whole Wheat Atta", "Toor Dal", "Moong Dal",
                 "Chana Dal", "Sugar", "Iodised Salt", "Sunflower Oil", "Mustard Oil", "Poha", "Besan"],
                ["500g", "1kg", "5kg"], (0.99, 14.99), 5),
    "Dairy": (["Toned Milk", "Full Cream Milk", "Curd", "Paneer", "Butter", "Cheese Slices", "Ghee",
               "Lassi", "Buttermilk"], ["200ml", "500ml", "1L", "200g", "500g"], (0.79, 9.99), 5),
    "Beverages": (["Green Tea", "Masala Tea", "Instant Coffee", "Orange Juice", "Mango Drink", "Cola",
                   "Lemon Soda", "Energy Drink", "Mineral Water"], ["250ml", "500ml", "1L", "2L", "100g", "250g"],
                  (0.69, 7.99), 12),
    "Snacks": (["Potato Chips", "Namkeen Mix", "Bhujia", "Cream Biscuits", "Marie Biscuits", "Cookies",
                "Instant Noodles", "Popcorn", "Chocolate Bar", "Roasted Peanuts"], ["50g", "100g", "200g", "400g"],
               (0.49, 4.99), 12),
    "Personal Care": (["Shampoo", "Soap Bar", "Toothpaste", "Face Wash", "Body Lotion", "Hair Oil",
                       "Deodorant", "Hand Wash"], ["75ml", "100ml", "200ml", "400ml", "Pack of 4"], (1.49, 12.99), 18),
    "Household": (["Detergent Powder", "Dishwash Liquid", "Floor Cleaner", "Toilet Cleaner",
                   "Garbage Bags", "Tissue Roll", "Mosquito Repellent"], ["500ml", "1L", "1kg", "2kg", "Pack of 6"],
                  (1.99, 14.99), 18),
    "Fresh Produce": (["Onion", "Potato", "Tomato", "Banana", "Apple", "Spinach", "Cauliflower", "Ginger",
                       "Green Chilli", "Lemon"], ["250g", "500g", "1kg"], (0.39, 4.99), 0),
    "Frozen": (["Frozen Peas", "Ice Cream Tub", "Frozen Paratha", "Veg Nuggets", "French Fries",
                "Frozen Corn"], ["200g", "400g", "500ml", "1L"], (1.99, 9.99), 18),
    "Baby Care": (["Baby Diapers", "Baby Wipes", "Baby Lotion", "Infant Cereal", "Baby Soap"],
                  ["Small", "Medium", "Large", "Pack of 2"], (3.99, 39.99), 12),
    "Home & Kitchen": (["Steel Tumbler", "Storage Container", "Non-stick Tawa", "LED Bulb",
                        "Plastic Bucket", "Kitchen Towel"], ["1pc", "Set of 3", "Set of 6"], (2.99, 249.99), 18),
}
BRANDS = ["Desi Harvest", "FreshNest", "Kisaan Gold", "UrbanKart", "Aarohi", "Sattva", "Prakriti",
          "DailyBasket", "HomeHero", "Nimbu", "Tara", "Aangan Select"]

FORMATS = {  # format -> (base lines per day, sqft range)
    "Hypermarket": (1500, (25000, 40000)),
    "Supermarket": (820, (8000, 15000)),
    "Express": (420, (1500, 3500)),
}

CANONICAL = ["bill_no", "bill_datetime", "store_code", "product_code", "quantity",
             "unit_price", "discount", "net_amount", "payment_mode"]

# Each layout maps CSV header -> canonical field (or "extra:<name>" for columns the warehouse can ignore).
LAYOUTS = {
    "A": {  # Standard POS v3 - the "contract" layout
        "pos_system": "POS-v3",
        "columns": [("bill_no", "bill_no"), ("bill_date", "bill_datetime"), ("store_code", "store_code"),
                    ("product_code", "product_code"), ("qty", "quantity"), ("unit_price", "unit_price"),
                    ("discount", "discount"), ("net_amount", "net_amount"), ("payment_mode", "payment_mode")],
        "date_fmt": "%Y-%m-%d %H:%M:%S",
        "filename": "{store}_{d:%Y%m%d}.csv",
        "encoding": "utf-8", "newline": "\n",
    },
    "B": {  # Legacy desktop POS: Windows export, BOM, CRLF, DD/MM dates, numeric store id, 1,234.50 amounts
        "pos_system": "LegacyPOS",
        "columns": [("BillDate", "bill_datetime"), ("BillNo", "bill_no"), ("StoreID", "store_code"),
                    ("ItemCode", "product_code"), ("ItemName", "extra:item_name"), ("Qty", "quantity"),
                    ("Rate", "unit_price"), ("Disc", "discount"), ("Amount", "net_amount"),
                    ("PayMode", "payment_mode")],
        "date_fmt": "%d/%m/%Y %H:%M",
        "filename": "sales_{store}_{d:%d-%m-%Y}.csv",
        "encoding": "utf-8-sig", "newline": "\r\n",
    },
    "C": {  # Cloud POS: different names, extra columns, 12-hour dates, fractional units
        "pos_system": "CloudPOS",
        "columns": [("invoice_id", "bill_no"), ("txn_ts", "bill_datetime"), ("outlet_code", "store_code"),
                    ("terminal_id", "extra:terminal_id"), ("sku", "product_code"), ("units", "quantity"),
                    ("selling_price", "unit_price"), ("discount_value", "discount"), ("line_total", "net_amount"),
                    ("tender_type", "payment_mode"), ("customer_id", "extra:customer_id"),
                    ("loyalty_points", "extra:loyalty_points")],
        "date_fmt": "%d-%b-%Y %I:%M %p",
        "filename": "{store}-{d:%Y-%m-%d}-export.csv",
        "encoding": "utf-8", "newline": "\n",
    },
    "D": {  # POS-v3 after a vendor upgrade: renamed columns + new columns (schema drift)
        "pos_system": "POS-v3",  # store master is NOT updated - that's the point
        "columns": [("bill_number", "bill_no"), ("bill_date", "bill_datetime"), ("store_code", "store_code"),
                    ("product_code", "product_code"), ("sale_qty", "quantity"), ("unit_price", "unit_price"),
                    ("discount", "discount"), ("gst_amount", "extra:gst_amount"), ("net_amount", "net_amount"),
                    ("payment_mode", "payment_mode"), ("terminal_id", "extra:terminal_id")],
        "date_fmt": "%Y-%m-%dT%H:%M:%S",
        "filename": "{store}_{d:%Y%m%d}.csv",
        "encoding": "utf-8", "newline": "\n",
    },
}

PAYMENT_REPR = {
    "A": {"CASH": "CASH", "UPI": "UPI", "CARD": "CARD", "WALLET": "WALLET"},
    "B": {"CASH": "Cash", "UPI": "UPI", "CARD": "Card", "WALLET": "E-Wallet"},
    "C": {"CASH": "cash", "UPI": "upi", "CARD": "credit_card", "WALLET": "wallet"},
    "D": {"CASH": "CASH", "UPI": "UPI", "CARD": "CARD", "WALLET": "WALLET"},
}

# Row-level defects (probability per clean line). These rows SHOULD be rejected.
ANOMALY_RATES = {
    "MISSING_REQUIRED_FIELD": 0.0030,
    "INVALID_QUANTITY": 0.0012,
    "INVALID_PRICE": 0.0008,
    "AMOUNT_MISMATCH": 0.0008,
    "UNKNOWN_PRODUCT": 0.0020,
    "UNKNOWN_STORE": 0.0004,
    "INVALID_DATE": 0.0004,
}
DUPLICATE_BILL_RATE = 0.0025  # whole bill re-emitted later in the same file
RETURN_BILL_RATE = 0.015
# Benign variations (should be standardised and LOADED)
MIXED_DATE_RATE = 0.006
STORE_CODE_VARIANT_RATE = 0.004
PAYMENT_CASE_VARIANT_RATE = 0.005
# File-level events
LATE_FILE_RATE = 0.05
RESEND_RATE = 0.03
RANDOM_MISSING_RATE = 0.010

HOUR_WEIGHTS = {9: 3, 10: 4, 11: 5, 12: 6, 13: 6, 14: 5, 15: 5, 16: 6, 17: 8, 18: 10, 19: 11, 20: 10, 21: 6}
PAYMENT_WEIGHTS = {"UPI": 45, "CASH": 28, "CARD": 22, "WALLET": 5}


# --------------------------------------------------------------------------------------
# Master data
# --------------------------------------------------------------------------------------
def build_stores(rng: random.Random, n: int) -> list[dict]:
    n_hyper = max(1, round(n * 0.2))
    n_express = max(1, round(n * 0.3)) if n > 2 else 0
    formats = ["Hypermarket"] * n_hyper + ["Express"] * n_express
    formats += ["Supermarket"] * (n - len(formats))
    rng.shuffle(formats)

    n_b = round(n * 0.25)
    n_c = round(n * 0.15)
    layouts = ["B"] * n_b + ["C"] * n_c + ["A"] * (n - n_b - n_c)
    rng.shuffle(layouts)

    stores = []
    for i in range(n):
        loc, zone = LOCALITIES[i % len(LOCALITIES)]
        fmt = formats[i]
        stores.append(make_store(rng, i + 1, loc, zone, fmt, layouts[i]))
    return stores


def make_store(rng, num, locality, zone, fmt, layout):
    lo, hi = FORMATS[fmt][1]
    opened = date(rng.randint(2014, 2025), rng.randint(1, 12), rng.randint(1, 28))
    return {
        "store_code": f"GGN{num:03d}",
        "store_name": f"{CHAIN_NAME} {fmt} - {locality}",
        "locality": locality,
        "zone": zone,
        "city": "Gurgaon",
        "state": "Haryana",
        "format": fmt,
        "pos_system": LAYOUTS[layout]["pos_system"],
        "open_date": opened.isoformat(),
        "sqft": rng.randrange(lo, hi, 500),
        "is_active": "Y",
        "_layout": layout,
    }


def build_products(rng: random.Random, n: int = 600) -> list[dict]:
    products, seen = [], set()
    cats = list(CATALOG.items())
    while len(products) < n:
        cat, (items, packs, (lo, hi), gst) = rng.choice(cats)
        brand, item, pack = rng.choice(BRANDS), rng.choice(items), rng.choice(packs)
        name = f"{brand} {item} {pack}"
        if name in seen:
            continue
        seen.add(name)
        # USD shelf prices end in .99 (e.g. 3.99); never below 0.49
        raw_price = math.exp(rng.uniform(math.log(lo), math.log(hi)))
        mrp = round(max(0.49, math.ceil(raw_price) - 0.01), 2)
        products.append({
            "product_code": f"SKU{len(products) + 1:05d}",
            "product_name": name,
            "category": cat,
            "brand": brand,
            "pack_size": pack,
            "mrp": f"{mrp:.2f}",
            "gst_rate": gst,
            "is_active": "Y",
        })
    # popularity: Zipf-like weights in random order
    weights = [1 / (r ** 0.9) for r in range(1, n + 1)]
    rng.shuffle(weights)
    for p, w in zip(products, weights):
        p["_weight"] = w
    return products


# --------------------------------------------------------------------------------------
# Plan of file-level events
# --------------------------------------------------------------------------------------
def build_plan(rng, stores, days):
    codes = [s["store_code"] for s in stores]
    plan = {"missing": set(), "header_only": set(), "zero_byte": set(), "revised": set(),
            "drift": {}, "header_noise": None}

    # Forced runs of consecutive missing days (should trigger alerts)
    runs = [2, 2, 3]
    for code, run in zip(rng.sample(codes, min(len(runs), len(codes))), runs):
        if days >= run + 6:
            start = rng.randint(3, days - run - 3)
            for d in range(start, start + run):
                plan["missing"].add((code, d))

    # Random single missing days (mostly no alert, unless they happen to line up)
    for code in codes:
        for d in range(1, days):
            if rng.random() < RANDOM_MISSING_RATE:
                plan["missing"].add((code, d))

    def pick_free(k):
        out, tries = set(), 0
        while len(out) < k and tries < 10_000:
            tries += 1
            c, d = rng.choice(codes), rng.randrange(1, max(2, days - 1))
            near = any((c, d + o) in plan["missing"] for o in (-1, 0, 1))
            taken = any((c, d) in plan[key] for key in ("header_only", "zero_byte", "revised"))
            if not near and not taken and (c, d) not in out:
                out.add((c, d))
        return out

    plan["header_only"] = pick_free(2)
    plan["zero_byte"] = pick_free(1)
    plan["revised"] = pick_free(3)

    a_stores = [s["store_code"] for s in stores if s["_layout"] == "A"]
    rng.shuffle(a_stores)
    drift_day = int(days * 0.6)
    for code in a_stores[:4]:
        plan["drift"][code] = drift_day
    if len(a_stores) > 4:
        plan["header_noise"] = a_stores[4]
    return plan


# --------------------------------------------------------------------------------------
# Line generation
# --------------------------------------------------------------------------------------
def target_lines(rng, store, d: date, day_idx, days):
    base = FORMATS[store["format"]][0]
    dow = d.weekday()
    f_dow = 1.2 if dow >= 5 else (1.05 if dow == 4 else 0.92)
    festive_start = days - 12
    f_fest = 1.0 + (0.25 * (day_idx - festive_start) / 12 if day_idx >= festive_start else 0)
    return max(20, int(base * f_dow * f_fest * rng.gauss(1.0, 0.08)))


def rand_time(rng, d: date) -> datetime:
    hour = rng.choices(list(HOUR_WEIGHTS), weights=list(HOUR_WEIGHTS.values()))[0]
    return datetime(d.year, d.month, d.day, hour, rng.randrange(60), rng.randrange(60))


def rand_qty(rng, category):
    if category == "Fresh Produce":
        return rng.choices([1, 2, 3], weights=[60, 30, 10])[0]
    r = rng.random()
    if r < 0.60:
        return 1
    if r < 0.85:
        return 2
    if r < 0.93:
        return 3
    if r < 0.98:
        return rng.randint(4, 6)
    return rng.randint(7, 12)


class BillNumberer:
    def __init__(self, rng):
        self.legacy_seq = rng.randint(100000, 800000)

    def next(self, layout, store, d, seq, is_return):
        if layout in ("A", "D"):
            return f"{'R' if is_return else ''}{store}{d:%y%m%d}{seq:04d}"
        if layout == "B":
            self.legacy_seq += 1
            return str(self.legacy_seq)  # unique per store only!
        return f"{'CRN' if is_return else 'INV'}-{store}-{d:%Y%m%d}-{seq:05d}"


def generate_lines(rng, store, layout, d, n_target, products, numberer):
    """Return a list of bills; each bill is a list of clean canonical line dicts."""
    bills, n, seq = [], 0, 0
    codes = [p["product_code"] for p in products]
    weights = [p["_weight"] for p in products]
    by_code = {p["product_code"]: p for p in products}
    times = []
    while n < n_target:
        k = rng.choices([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        weights=[30, 22, 16, 12, 9, 6, 2, 1.5, 1, 0.5])[0]
        times.append(k)
        n += k
    stamps = sorted(rand_time(rng, d) for _ in times)
    for k, ts in zip(times, stamps):
        seq += 1
        is_return = rng.random() < RETURN_BILL_RATE
        if is_return:
            k = rng.choice([1, 1, 2])
        bill_no = numberer.next(layout, store["store_code"], d, seq, is_return)
        pay = rng.choices(list(PAYMENT_WEIGHTS), weights=list(PAYMENT_WEIGHTS.values()))[0]
        terminal = f"T{rng.randint(1, 6 if store['format'] == 'Hypermarket' else 3):02d}"
        customer = f"C{rng.randint(10_000_000, 99_999_999)}" if rng.random() < 0.4 else ""
        picked = set()
        lines = []
        while len(lines) < k:
            code = rng.choices(codes, weights=weights)[0]
            if code in picked:  # (store, bill_no, product_code) is a natural key
                continue
            picked.add(code)
            p = by_code[code]
            mrp = float(p["mrp"])
            price = round(mrp * (1 - rng.choice([0, 0, 0, 0, 0.05, 0.10])), 2)
            if is_return:
                qty = -rng.choice([1, 1, 1, 2])
                disc = 0.0
            else:
                qty = rand_qty(rng, p["category"])
                disc = round(qty * price * rng.uniform(0.05, 0.20), 2) if rng.random() < 0.3 else 0.0
            net = round(qty * price - disc, 2)
            lines.append({
                "bill_no": bill_no, "dt": ts, "store_code": store["store_code"], "product_code": code,
                "product_name": p["product_name"], "quantity": qty, "unit_price": price, "discount": disc,
                "net_amount": net, "payment_mode": pay, "terminal_id": terminal, "customer_id": customer,
                "gst_rate": p["gst_rate"], "is_return": is_return,
            })
        bills.append(lines)
    return bills


# --------------------------------------------------------------------------------------
# Rendering (canonical -> layout-specific strings) and defect injection
# --------------------------------------------------------------------------------------
def money(x: float, layout: str) -> str:
    if layout == "B" and abs(x) >= 1000:
        return f"{x:,.2f}"  # $1,234.50 -> "1,234.50", quoted by the csv writer
    return f"{x:.2f}"


def store_repr(code: str, layout: str) -> str:
    return str(int(code[3:])) if layout == "B" else code


def render_line(line, layout):
    L = LAYOUTS[layout]
    q = line["quantity"]
    vals = {
        "bill_no": line["bill_no"],
        "bill_datetime": line["dt"].strftime(L["date_fmt"]),
        "store_code": store_repr(line["store_code"], layout),
        "product_code": line["product_code"],
        "quantity": f"{q:.1f}" if layout == "C" else str(q),
        "unit_price": money(line["unit_price"], layout),
        "discount": money(line["discount"], layout),
        "net_amount": money(line["net_amount"], layout),
        "payment_mode": PAYMENT_REPR[layout][line["payment_mode"]],
        "extra:item_name": line["product_name"][:24],  # legacy POS truncates names
        "extra:terminal_id": line["terminal_id"],
        "extra:customer_id": line["customer_id"],
        "extra:loyalty_points": str(int(max(line["net_amount"], 0) // 100)) if line["customer_id"] else "0",
        "extra:gst_amount": f"{line['net_amount'] - line['net_amount'] / (1 + line['gst_rate'] / 100):.2f}",
    }
    return vals


def apply_benign(rng, vals, line, layout, counter):
    if layout in ("A", "D") and rng.random() < MIXED_DATE_RATE:
        vals["bill_datetime"] = line["dt"].strftime("%d-%m-%Y %H:%M")
        counter["MIXED_DATE_FORMAT"] += 1
    if rng.random() < STORE_CODE_VARIANT_RATE:
        if layout == "B":
            vals["store_code"] = line["store_code"][3:]  # zero-padded "007"
        else:
            vals["store_code"] = rng.choice([f" {line['store_code'].lower()}", f"{line['store_code']} "])
        counter["STORE_CODE_VARIANT"] += 1
    if layout in ("A", "D") and rng.random() < PAYMENT_CASE_VARIANT_RATE:
        vals["payment_mode"] = vals["payment_mode"].title()
        counter["PAYMENT_MODE_VARIANT"] += 1


def apply_defect(rng, vals, line, layout):
    """Possibly corrupt the row. Returns (anomaly_code, detail) or (None, None)."""
    r = rng.random()
    acc = 0.0
    code = None
    for c, p in ANOMALY_RATES.items():
        acc += p
        if r < acc:
            code = c
            break
    if code is None:
        return None, None
    blank = rng.choice(["", "", "NULL", "NA"]) if layout == "B" else ""
    if code == "MISSING_REQUIRED_FIELD":
        field = rng.choice(["bill_no", "product_code", "quantity", "unit_price", "net_amount", "bill_datetime"])
        vals[field] = blank
        return code, f"{field} is blank ({blank!r})"
    if code == "INVALID_QUANTITY":
        bad = rng.choice(["0", "9999", "-999", "1500"])
        vals["quantity"] = bad + (".0" if layout == "C" else "")
        return code, f"quantity={bad}"
    if code == "INVALID_PRICE":
        price = line["unit_price"]
        bad = rng.choice([-price, 0.0, round(price * 100, 2)])
        vals["unit_price"] = money(bad, layout)
        return code, f"unit_price={bad} (mrp-based limit violated)"
    if code == "AMOUNT_MISMATCH":
        net = line["net_amount"]
        bad = round(-net, 2) if rng.random() < 0.3 else round(net * rng.uniform(1.5, 3.0), 2)
        vals["net_amount"] = money(bad, layout)
        return code, f"net_amount={bad} expected={net}"
    if code == "UNKNOWN_PRODUCT":
        bad = rng.choice([f"SKU9{rng.randint(1000, 9999)}", f"SKU{rng.randint(70000, 79999)}", "TEMP-ITEM"])
        vals["product_code"] = bad
        return code, f"product_code={bad}"
    if code == "UNKNOWN_STORE":
        if layout == "B":
            bad = str(rng.randint(90, 99))
        else:
            sc = line["store_code"]
            bad = rng.choice([f"GGN{rng.randint(90, 99):03d}", sc[:4] + "O" + sc[5:], "GN" + sc[3:]])
        vals["store_code"] = bad
        return code, f"store_code={bad}"
    if code == "INVALID_DATE":
        bad = rng.choice(["31-02-2026 11:15", "2026-13-45 10:00:00", "##########", "0000-00-00 00:00:00",
                          line["dt"].replace(year=2062).strftime(LAYOUTS[layout]["date_fmt"])])
        vals["bill_datetime"] = bad
        return code, f"bill_datetime={bad}"
    return None, None


def build_file_rows(rng, bills, layout, counter):
    """Render bills -> ordered rows [(values, anomaly_code, detail, line)] and the clean lines."""
    cols = LAYOUTS[layout]["columns"]
    rendered_bills = []
    clean_lines = []
    for lines in bills:
        rows = []
        for line in lines:
            vals = render_line(line, layout)
            apply_benign(rng, vals, line, layout, counter)
            code, detail = apply_defect(rng, vals, line, layout)
            if code is None:
                clean_lines.append(line)
            rows.append(([vals[c] for _, c in cols], code, detail, line))
        rendered_bills.append(rows)

    # whole-bill duplicates re-emitted a little later in the file
    inserts = defaultdict(list)
    nb = len(rendered_bills)
    for b, rows in enumerate(rendered_bills):
        if rng.random() < DUPLICATE_BILL_RATE and b < nb - 1:
            at = rng.randint(b + 1, min(b + 40, nb - 1))
            for row, _, _, line in rows:
                inserts[at].append((list(row), "DUPLICATE_ROW", f"copy of bill {line['bill_no']}", line))

    ordered = []
    for b, rows in enumerate(rendered_bills):
        ordered.extend(rows)
        ordered.extend(inserts.get(b, []))
    return ordered, clean_lines


def write_csv(path: Path, layout: str, rows, header_noise=False):
    L = LAYOUTS[layout]
    header = [h for h, _ in L["columns"]]
    if header_noise:
        header = [rng_free_noise(h, i) for i, h in enumerate(header)]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding=L["encoding"]) as fh:
        w = csv.writer(fh, lineterminator=L["newline"])
        w.writerow(header)
        w.writerows(rows)


def rng_free_noise(h: str, i: int) -> str:
    # deterministic header noise: UPPER CASE with stray whitespace
    return (" " if i % 3 == 1 else "") + h.upper() + (" " if i % 2 == 0 else "")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def with_suffix(name: str, suffix: str) -> str:
    stem, ext = name.rsplit(".", 1)
    return f"{stem}{suffix}.{ext}"


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="data", help="output root (default: data)")
    ap.add_argument("--stores", type=int, default=40)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--start", default=START_DATE.isoformat(), help="first business date (YYYY-MM-DD)")
    ap.add_argument("--products", type=int, default=600)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    out = Path(args.out)
    landing, truth, master = out / "landing", out / "_ground_truth", out / "master"
    for p in (landing, truth):
        if p.exists():
            shutil.rmtree(p)
    for p in (landing, truth, master):
        p.mkdir(parents=True, exist_ok=True)

    start = date.fromisoformat(args.start)
    rng = random.Random(args.seed)
    stores = build_stores(rng, args.stores)
    products = build_products(rng, args.products)
    plan = build_plan(rng, stores, args.days)

    # ---- master data
    store_cols = [k for k in stores[0] if not k.startswith("_")]
    with open(master / "stores.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=store_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(stores)
    prod_cols = [k for k in products[0] if not k.startswith("_")]
    with open(master / "products.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=prod_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(products)

    # unknown store: a new outlet that started sending files before being added to the master
    unknown_store = make_store(random.Random(f"{args.seed}-unknown"), args.stores + 1,
                               *UNKNOWN_STORE_LOCALITY, "Express", "A")
    unknown_days = set(range(max(0, args.days - 6), args.days))

    manifest, anomalies, totals = [], [], []
    benign = Counter()
    anomaly_counts = Counter()
    total_rows = 0

    def emit(store, d_idx, rows, layout, name, arrival: date, event, header_noise=False):
        nonlocal total_rows
        d = start + timedelta(days=d_idx)
        path = landing / arrival.isoformat() / name
        write_csv(path, layout, [r[0] for r in rows], header_noise=header_noise)
        manifest.append({
            "file_name": name, "landing_folder": arrival.isoformat(), "store_code": store["store_code"],
            "business_date": d.isoformat(), "layout": layout, "event": event, "data_rows": len(rows),
            "bytes": path.stat().st_size, "sha256": sha256(path),
        })
        total_rows += len(rows)
        return path

    all_stores = stores + [unknown_store]
    for store in all_stores:
        code = store["store_code"]
        numberer = BillNumberer(random.Random(f"{args.seed}-{code}-bills"))
        for d_idx in range(args.days):
            if code == unknown_store["store_code"] and d_idx not in unknown_days:
                continue
            d = start + timedelta(days=d_idx)
            frng = random.Random(f"{args.seed}-{code}-{d_idx}")
            layout = store["_layout"]
            if code in plan["drift"] and d_idx >= plan["drift"][code]:
                layout = "D"
            if (code, d_idx) in plan["missing"]:
                continue  # store sent nothing for this business date
            name = LAYOUTS[layout]["filename"].format(store=code, d=d)
            on_time = d + timedelta(days=1)
            late = frng.random() < LATE_FILE_RATE
            arrival = d + timedelta(days=frng.randint(2, 4)) if late else on_time
            event = "LATE" if late else "ON_TIME"
            header_noise = code == plan["header_noise"]

            if (code, d_idx) in plan["header_only"]:
                emit(store, d_idx, [], layout, name, arrival, "HEADER_ONLY")
                totals.append(_totals(code, d, []))
                continue

            n_target = target_lines(frng, store, d, d_idx, args.days)
            bills = generate_lines(frng, store, layout, d, n_target, products, numberer)
            rows, clean = build_file_rows(frng, bills, layout, benign)
            is_unknown = code == unknown_store["store_code"]

            if (code, d_idx) in plan["zero_byte"]:
                zpath = landing / arrival.isoformat() / name
                zpath.parent.mkdir(parents=True, exist_ok=True)
                zpath.write_bytes(b"")
                manifest.append({"file_name": name, "landing_folder": arrival.isoformat(), "store_code": code,
                                 "business_date": d.isoformat(), "layout": layout, "event": "ZERO_BYTE",
                                 "data_rows": 0, "bytes": 0, "sha256": sha256(zpath)})
                arrival = arrival + timedelta(days=1)
                event = "RESENT_AFTER_ZERO_BYTE"

            final_clean = clean
            path = emit(store, d_idx, rows, layout, name, arrival,
                        "UNKNOWN_STORE" if is_unknown else event, header_noise=header_noise)

            if (code, d_idx) in plan["revised"]:
                # A corrected re-export: some bills voided, some quantities fixed. Latest version wins.
                v2_bills = []
                for lines in bills:
                    if frng.random() < 0.02:
                        continue
                    new_lines = []
                    for ln in lines:
                        ln = dict(ln)
                        if not ln["is_return"] and frng.random() < 0.01:
                            ln["quantity"] += 1
                            ln["net_amount"] = round(ln["quantity"] * ln["unit_price"] - ln["discount"], 2)
                        new_lines.append(ln)
                    v2_bills.append(new_lines)
                v2_rows, v2_clean = build_file_rows(frng, v2_bills, layout, benign)
                v2_arrival = arrival + timedelta(days=2)
                emit(store, d_idx, v2_rows, layout, with_suffix(name, "_v2"), v2_arrival, "REVISED_V2",
                     header_noise=header_noise)
                _record_anomalies(anomalies, anomaly_counts, rows, name, code, d, superseded=True)
                final_clean = v2_clean
                _record_anomalies(anomalies, anomaly_counts, v2_rows, with_suffix(name, "_v2"), code, d)
            else:
                _record_anomalies(anomalies, anomaly_counts, rows, name, code, d,
                                  whole_file="UNKNOWN_STORE" if is_unknown else None)

            if frng.random() < RESEND_RATE and not is_unknown:
                mode = frng.choice(["same_name_later", "resend_suffix", "copy_suffix"])
                if mode == "same_name_later":
                    dest, dup_name = arrival + timedelta(days=frng.randint(1, 2)), name
                elif mode == "resend_suffix":
                    dest, dup_name = arrival + timedelta(days=1), with_suffix(name, "_resend")
                else:
                    dest, dup_name = arrival, with_suffix(name, " (1)")
                dpath = landing / dest.isoformat() / dup_name
                dpath.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, dpath)
                manifest.append({"file_name": dup_name, "landing_folder": dest.isoformat(), "store_code": code,
                                 "business_date": d.isoformat(), "layout": layout, "event": "DUPLICATE_DELIVERY",
                                 "data_rows": len(rows), "bytes": dpath.stat().st_size, "sha256": sha256(dpath)})
                total_rows += len(rows)

            if not is_unknown:
                totals.append(_totals(code, d, final_clean))

    # ---- missing days & expected alerts (rule: no file with >=1 valid row for a business date)
    no_data = defaultdict(set)
    for (code, d_idx) in plan["missing"] | plan["header_only"]:
        no_data[code].add(d_idx)
    missing_rows, alert_rows = [], []
    for code in sorted(no_data):
        days_sorted = sorted(no_data[code])
        for d_idx in days_sorted:
            missing_rows.append({"store_code": code, "business_date": (start + timedelta(days=d_idx)).isoformat(),
                                 "reason": "HEADER_ONLY_FILE" if (code, d_idx) in plan["header_only"] else "NO_FILE"})
        run = [days_sorted[0]]
        for d_idx in days_sorted[1:] + [None]:
            if d_idx is not None and d_idx == run[-1] + 1:
                run.append(d_idx)
                continue
            if len(run) >= 2:
                alert_rows.append({"store_code": code,
                                   "first_missing_date": (start + timedelta(days=run[0])).isoformat(),
                                   "last_missing_date": (start + timedelta(days=run[-1])).isoformat(),
                                   "consecutive_days": len(run),
                                   "alert_on_date": (start + timedelta(days=run[1])).isoformat()})
            if d_idx is not None:
                run = [d_idx]

    # ---- write ground truth
    _write(truth / "file_manifest.csv", sorted(manifest, key=lambda r: (r["landing_folder"], r["file_name"])))
    _write(truth / "injected_anomalies.csv", anomalies,
           ["file_name", "store_code", "business_date", "row_number", "anomaly_code", "detail", "superseded"])
    _write(truth / "expected_daily_totals.csv", sorted(totals, key=lambda r: (r["store_code"], r["business_date"])))
    _write(truth / "missing_store_days.csv", missing_rows, ["store_code", "business_date", "reason"])
    _write(truth / "expected_alerts.csv", alert_rows,
           ["store_code", "first_missing_date", "last_missing_date", "consecutive_days", "alert_on_date"])

    events = Counter(m["event"] for m in manifest)
    effective_rows = sum(m["data_rows"] for m in manifest
                         if m["event"] not in ("DUPLICATE_DELIVERY", "ZERO_BYTE"))
    effective_rejects = sum(v for k, v in anomaly_counts.items() if not k.startswith("superseded:"))
    summary = {
        "seed": args.seed, "stores_in_master": len(stores), "products_in_master": len(products),
        "business_dates": [start.isoformat(), (start + timedelta(days=args.days - 1)).isoformat()],
        "files_delivered": len(manifest), "file_events": dict(events),
        "total_rows_in_landing": total_rows,
        "rows_excluding_duplicate_deliveries": effective_rows,
        "expected_valid_lines": sum(t["expected_lines"] for t in totals),
        "expected_net_sales": round(sum(t["expected_net_amount"] for t in totals), 2),
        "row_anomalies": {k: v for k, v in sorted(anomaly_counts.items())},
        "expected_reject_rate_pct": round(100 * effective_rejects / max(1, effective_rows), 3),
        "benign_variations_to_standardise": dict(benign),
        "schema_drift": {c: (start + timedelta(days=d)).isoformat() for c, d in plan["drift"].items()},
        "header_noise_store": plan["header_noise"],
        "unknown_store": unknown_store["store_code"],
        "missing_store_days": len(missing_rows), "expected_alerts": len(alert_rows),
    }
    (truth / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def _totals(code, d, clean_lines):
    sales = [l for l in clean_lines if not l["is_return"]]
    rets = [l for l in clean_lines if l["is_return"]]
    return {
        "store_code": code, "business_date": d.isoformat(),
        "expected_lines": len(clean_lines),
        "expected_bills": len({l["bill_no"] for l in clean_lines}),
        "expected_units": sum(l["quantity"] for l in clean_lines),
        "expected_gross_amount": round(sum(l["quantity"] * l["unit_price"] for l in clean_lines), 2),
        "expected_discount": round(sum(l["discount"] for l in clean_lines), 2),
        "expected_net_amount": round(sum(l["net_amount"] for l in clean_lines), 2),
        "expected_sales_net": round(sum(l["net_amount"] for l in sales), 2),
        "expected_returns_net": round(sum(l["net_amount"] for l in rets), 2),
    }


def _record_anomalies(out, counter, rows, name, code, d, superseded=False, whole_file=None):
    for i, (_, acode, detail, _) in enumerate(rows, start=1):
        if whole_file:
            acode, detail = whole_file, "store not in store master"
        if acode is None:
            continue
        out.append({"file_name": name, "store_code": code, "business_date": d.isoformat(), "row_number": i,
                    "anomaly_code": acode, "detail": detail, "superseded": "Y" if superseded else "N"})
        counter[f"superseded:{acode}" if superseded else acode] += 1


def _write(path: Path, rows, fields=None):
    fields = fields or (list(rows[0]) if rows else [])
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
