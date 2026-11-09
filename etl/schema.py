"""Map a file's raw headers onto the canonical schema contract, and detect schema drift.

Steps for each file:
  1. normalise every header ("  BILL_NO " -> "bill_no", "Sale Qty" -> "sale_qty")
  2. look each one up in the contract's alias lists
  3. headers that match nothing are "extra" columns: ignored for loading but logged
  4. if a required column is still missing, optionally fuzzy-match it against the unknown headers
     (difflib ratio >= cutoff); a file that still misses a required column is rejected
  5. a layout signature (hash of the sorted normalised headers) lets us notice when a store
     changes layout between days
"""
from __future__ import annotations

import difflib
import hashlib
import re
from dataclasses import dataclass, field


@dataclass
class HeaderMapping:
    mapping: dict[str, int]                 # canonical column -> index of the raw column
    raw_headers: list[str]
    extras: list[str] = field(default_factory=list)
    missing_required: list[str] = field(default_factory=list)
    auto_mapped: dict[str, str] = field(default_factory=dict)   # canonical -> raw header (fuzzy)
    duplicates: list[str] = field(default_factory=list)
    signature: str = ""

    @property
    def ok(self) -> bool:
        return not self.missing_required

    def source_header(self, canonical: str) -> str | None:
        idx = self.mapping.get(canonical)
        return self.raw_headers[idx] if idx is not None else None


def normalise_header(header: str) -> str:
    """'﻿ BillNo ' -> 'billno', 'Sale Qty' -> 'sale_qty', 'line-total' -> 'line_total'."""
    h = header.replace("﻿", "").strip().lower()
    h = re.sub(r"[\s\-./]+", "_", h)
    return re.sub(r"_+", "_", h).strip("_")


def layout_signature(headers: list[str]) -> str:
    norm = sorted(normalise_header(h) for h in headers)
    return hashlib.sha1("|".join(norm).encode()).hexdigest()[:12]


def build_alias_index(contract: dict[str, dict]) -> dict[str, str]:
    index: dict[str, str] = {}
    for canonical, spec in contract.items():
        for alias in [canonical, *spec.get("aliases", [])]:
            index[normalise_header(alias)] = canonical
    return index


def map_headers(headers: list[str], contract: dict[str, dict], fuzzy: bool = True,
                cutoff: float = 0.85) -> HeaderMapping:
    alias_index = build_alias_index(contract)
    result = HeaderMapping(mapping={}, raw_headers=list(headers), signature=layout_signature(headers))
    unknown: dict[str, int] = {}
    for i, raw in enumerate(headers):
        norm = normalise_header(raw)
        canonical = alias_index.get(norm)
        if canonical is None:
            unknown[norm] = i
        elif canonical in result.mapping:
            result.duplicates.append(raw)           # two headers map to the same column: keep the first
        else:
            result.mapping[canonical] = i

    missing = [c for c, spec in contract.items() if spec.get("required") and c not in result.mapping]
    if fuzzy and missing and unknown:
        for canonical in list(missing):
            candidates = [normalise_header(a) for a in [canonical, *contract[canonical].get("aliases", [])]]
            best, best_score = None, 0.0
            for norm in unknown:
                score = max(difflib.SequenceMatcher(None, norm, c).ratio() for c in candidates)
                if score > best_score:
                    best, best_score = norm, score
            if best is not None and best_score >= cutoff:
                idx = unknown.pop(best)
                result.mapping[canonical] = idx
                result.auto_mapped[canonical] = headers[idx]
                missing.remove(canonical)

    result.missing_required = missing
    result.extras = [headers[i] for i in unknown.values()]
    return result
