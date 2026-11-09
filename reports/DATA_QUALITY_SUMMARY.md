# Data Quality Summary

_Generated 2026-11-09T03:46:33+00:00 from the `sqlite` warehouse (last run `20261109T034628943`, 0.48 s)._

## Headline

| Metric | Value |
|---|---|
| Last full rerun | 152.47 s for 2,448 files (target < 600 s) |
| Rows read (current files) | 2,047,269 |
| Rows loaded | 2,021,800 |
| Rows rejected | 25,469 |
| **Reject rate** | **1.244%** (target < 2%) |
| Partitions reconciled within 0.5% | 2,364 / 2,364 |
| Max reconciliation variance | 0.0% |
| Source net (all rows) | $39,620,192.18 |
| Rejected net | $421,145.17 |
| Warehouse net | $39,199,047.01 |

## Rejected rows by reason

| Reason code | Rows | % of rows read | Files affected |
|---|---:|---:|---:|
| `MISSING_REQUIRED_FIELD` | 6,095 | 0.298% | 2,059 |
| `DUPLICATE_ROW` | 4,943 | 0.241% | 1,209 |
| `UNKNOWN_PRODUCT` | 4,153 | 0.203% | 1,841 |
| `UNKNOWN_STORE` | 3,751 | 0.183% | 698 |
| `INVALID_QUANTITY` | 2,472 | 0.121% | 1,434 |
| `INVALID_PRICE` | 1,633 | 0.080% | 1,130 |
| `AMOUNT_MISMATCH` | 1,612 | 0.079% | 1,128 |
| `INVALID_DATE` | 810 | 0.040% | 664 |

## Files by status

| Status | Reason | Files |
|---|---|---:|
| LOADED |  | 2,364 |
| SKIPPED_DUPLICATE | DUPLICATE_FILE | 72 |
| QUARANTINED | UNKNOWN_STORE_FILE | 6 |
| SUPERSEDED |  | 3 |
| EMPTY | EMPTY_FILE | 2 |
| FAILED | ZERO_BYTE_FILE | 1 |

## Missing-store alerts

| Store | First missing | Last missing | Days |
|---|---|---|---:|
| GGN006 | 2026-09-18 | 2026-09-19 | 2 |
| GGN015 | 2026-10-21 | 2026-10-22 | 2 |
| GGN023 | 2026-09-23 | 2026-09-25 | 3 |

## Stores with the highest reject rate

| Store | Rows read | Rejected | Reject % |
|---|---:|---:|---:|
| GGN041 | 2,906 | 2,906 | 100.00% |
| GGN037 | 25,939 | 344 | 1.33% |
| GGN003 | 24,897 | 300 | 1.20% |
| GGN021 | 50,499 | 603 | 1.19% |
| GGN005 | 49,398 | 586 | 1.19% |

## Success criteria

- ✅ reconciliation within 0 5 pct
- ✅ reject rate below 2 pct
- ✅ run under 10 minutes
