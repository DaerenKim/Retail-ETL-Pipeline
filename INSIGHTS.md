# INSIGHTS: How this project works, and what it taught

This document explains the Retail ETL Pipeline from end to end: the problem, how it was built step by step, what every file does, which tools were used and why, the bugs that came up, the results, and the lessons. It is written for someone who wants to understand the project well enough to explain it, extend it, or defend it in an interview.

The [README](README.md) is the reference manual. This file is the story behind it.

---

## Contents
1. [TL;DR](#1-tldr)
2. [The problem in plain words](#2-the-problem-in-plain-words)
3. [The build, step by step](#3-the-build-step-by-step)
4. [Following one file through the pipeline](#4-following-one-file-through-the-pipeline)
5. [What every file does](#5-what-every-file-does)
6. [Tools used, and why](#6-tools-used-and-why)
7. [Key design decisions and trade-offs](#7-key-design-decisions-and-trade-offs)
8. [Bugs found along the way](#8-bugs-found-along-the-way)
9. [Results](#9-results)
10. [What the data says (business view)](#10-what-the-data-says-business-view)
11. [Lessons learnt](#11-lessons-learnt)
12. [Limitations and next steps](#12-limitations-and-next-steps)
13. [Talking about this project in an interview](#13-talking-about-this-project-in-an-interview)
14. [Glossary](#14-glossary)

---

## 1. TL;DR

- **What:** a Python pipeline that takes 60 days of messy daily sales CSVs from 40 stores (about 2.1 million rows, 4 file layouts) and turns them into a clean, trustworthy sales warehouse.
- **How:** DuckDB cleans and validates each file with SQL. Each store-day is loaded **as one unit inside one database transaction**, so re-running can never double-count. Every rejected row is kept with a reason code.
- **Proof:** a data generator built alongside the pipeline injects every problem on purpose and writes an answer key. The pipeline matches the answer key **exactly**: 2,021,800 loaded rows, 25,469 rejected rows with the right reason for each, and all 3 missing-store alerts.
- **Targets met:** reconciliation 2,364/2,364 store-days within 0.5% (actual difference 0.00), reject rate 1.244% (< 2%), full rerun 152 s (< 10 min).
- **Extras:** PostgreSQL support, a Parquet staging layer, dbt models with 24 data tests, a Streamlit dashboard, 116 automated tests, GitHub Actions CI.

## 2. The problem in plain words

A retail chain's head office wants to know, every morning, how much each store sold yesterday. Each store's till system sends a CSV file. In real life those files are a mess:

| What goes wrong | Why it hurts |
|---|---|
| Stores use **different till software**, so different column names, date formats and number formats | A naive loader breaks, or worse, reads `01/09` as January 9th |
| Files arrive **late**, or **twice**, or as a **corrected version** | Naive loading double-counts sales or loses the correction |
| Some rows are **broken** (blank product, quantity 9999, price above the shelf price) | Bad rows corrupt the numbers if loaded, and silently disappear if dropped |
| **Returns** have negative quantities | They look like errors but are real money going back to customers |
| Some stores **send nothing** | Head office needs to know, rather than see a quietly low total |
| A till upgrade **renames columns** mid-month | The pipeline must adapt rather than crash |

So the job is not just "load CSVs into a database". It is to **load them in a way the business can trust**, and to **prove** that it can be trusted.

## 3. The build, step by step

### Step 1: Design the dataset first

There was no real data, so the first step was a **data generator** ([generator/generate_data.py](generator/generate_data.py)). Designing the data first forced every requirement in the brief to become concrete:

- 40 fictional stores across Gurgaon localities, in three formats (Hypermarket / Supermarket / Express) with different volumes.
- 600 fictional products in 10 categories, with USD shelf prices ending in .99.
- Realistic patterns: weekends +25%, a festive ramp at the end of October, evening peaks, about 1.5% of bills being returns.
- **Four till layouts** with every annoying difference real systems have: a byte-order mark (BOM), Windows line endings, DD/MM dates, `"1,247.94"` amounts, numeric store IDs, extra columns.
- **Injected defects** at controlled rates, and **file events**: late, duplicate, `_v2`, empty, zero-byte, unknown store, missing days.

The most valuable decision was making the generator **write an answer key** (`data/_ground_truth/`) listing every defect it injected and the correct daily totals. That turned "does the pipeline work?" from an opinion into an automated, exact comparison.

### Step 2: Write the contract and the config

Before any pipeline code: [config/schema_contract.yaml](config/schema_contract.yaml) defines the 9 columns every file must map to and the aliases each may arrive under. [config/pipeline.yaml](config/pipeline.yaml) holds everything that might change: paths, thresholds, filename patterns, date formats, the payment mapping. The rule followed throughout was **no magic numbers in code**.

### Step 3: Build the stages as small modules

Each stage became one small file with one job: discover → fingerprint → extract → map schema → standardise → validate → load → reconcile and alert. Small modules can each be tested on their own, and a bug in one stage is easy to locate.

### Step 4: Make loading idempotent

This is the heart of the project; see §7.1. The unit of loading is a **store-day partition**, replaced in a single transaction, with a registry of file checksums in front of it.

### Step 5: Test against the answer key

A small dataset (8 stores × 14 days) was generated and run through, then compared with the answer key. It matched on the first full comparison. Next came the full 2.1M-row run (2.5 minutes) and a second run to confirm it loaded nothing.

### Step 6: Write the tests and let them find bugs

116 tests were written: unit tests for every rule, plus end-to-end tests that generate data and check the pipeline against the answer key. They found **real bugs** that the hand-run had missed (see §8).

### Step 7: PostgreSQL

The database code goes through a thin layer ([etl/warehouse.py](etl/warehouse.py)), so switching to PostgreSQL is a config change. A throwaway PostgreSQL 16 server was started locally and the same tests passed. A full 2.1M-row load took 154 s with identical results.

### Step 8: Proving incremental behaviour

The pipeline was run **62 times**, once per arrival folder, the way it would run each morning in production. The final warehouse was identical to one big run. Along the way, 7 temporary "missing store" alerts were raised and then **auto-resolved** when late files turned up. That is exactly the behaviour you want from an alerting system.

### Step 9: Analytics layer and dashboard

dbt models turn the clean Parquet data into business tables (daily store sales, store scorecard, payment mix, top products), with 24 data tests. A Streamlit dashboard sits on top of them. The dbt total matched the warehouse to the cent, an independent third confirmation.

### Step 10: CI and documentation

GitHub Actions runs everything on every push, with a real PostgreSQL service container. Then this document and the README were written.

### Step 11: Currency switched to USD

Late in the project the currency was changed from rupees to US dollars. That was more than relabelling:

- product prices were regenerated in dollars;
- the amount tolerance was tightened from ₹1.00 to **$0.05**, because a $1 tolerance on a $0.99 item would hide real errors;
- the whole run was repeated and re-verified against the answer key.

A currency change is a **business rule** change, not a cosmetic one.

## 4. Following one file through the pipeline

Take `data/landing/2026-09-02/sales_GGN008_01-09-2026.csv`, a legacy-POS file for store 8 on 1 September:

```
BillDate,BillNo,StoreID,ItemCode,ItemName,Qty,Rate,Disc,Amount,PayMode
01/09/2026 10:29,194656,8,SKU00258,DailyBasket Plastic Buck,6,207.99,0.00,"1,247.94",Card
```

1. **Discover** ([discover.py](etl/discover.py)): the filename matches the `legacy_pos` regex, giving store `GGN008`, business date `2026-09-01`, version 1.
2. **Fingerprint** ([fingerprint.py](etl/fingerprint.py)): compute the SHA-256 of the bytes. If `(path, sha)` is already in `etl_file_log`, skip it immediately. If the same bytes were already loaded for this store-day under another name, record `SKIPPED_DUPLICATE`.
3. **Extract** ([extract.py](etl/extract.py)): Python's `csv` module reads the header (and strips the BOM). DuckDB reads the body with **every column as text**, named `c0…c9`, and numbers the rows. Nothing is converted yet, so nothing can be silently lost.
4. **Map schema** ([schema.py](etl/schema.py)): `BillDate` maps to `bill_datetime`, `StoreID` to `store_code`, `Amount` to `net_amount`, and so on. `ItemName` is an extra column, so it is ignored.
5. **Standardise** ([transform.py](etl/transform.py)): one SQL query cleans the whole file:
   - `01/09/2026 10:29` is tried against each configured format and parses as 1 Sep 10:29;
   - `8` becomes `GGN008`;
   - `"1,247.94"` becomes `1247.94`;
   - `Card` becomes `CARD`.
6. **Validate** ([validate.py](etl/validate.py)): the rules run in order. Is the store in the master and the same as the filename's? Is the product known? Is 207.99 at or below the MRP? Does 6 × 207.99 − 0 = 1,247.94? All pass, so the row's reason code is NULL and it will be loaded.
7. **Load** ([load.py](etl/load.py)), in one transaction:
   1. delete any existing rows for `(GGN008, 2026-09-01)` in `fact_sales` and `etl_rejected_rows`;
   2. record the file in `etl_file_log`;
   3. insert the valid rows and the rejected rows;
   4. mark this file as the owner of the partition in `etl_partition`;
   5. commit.
8. **Silver**: write the clean rows to `data/silver/sales/store_code=GGN008/business_date=2026-09-01/data.parquet`.
9. **After all files:** recompute missing-store alerts ([alerts.py](etl/alerts.py)), then re-read every source file a second, independent way and reconcile it against the warehouse ([reconcile.py](etl/reconcile.py)).

If a later `sales_GGN008_01-09-2026_v2.csv` arrives, step 7 simply replaces the partition. The old file is marked `SUPERSEDED` and the totals stay correct.

## 5. What every file does

### Configuration
| File | Purpose |
|---|---|
| `config/pipeline.yaml` | All settings: paths, warehouse URL, store list, filename regexes, date formats, payment map, null tokens, thresholds, drift and alert settings |
| `config/schema_contract.yaml` | The 9 canonical columns, their types, required flags and header aliases |

### The pipeline package (`etl/`)
| File | Purpose | Key functions |
|---|---|---|
| `__main__.py` / `cli.py` | Command-line entry: `python -m etl init-db / run / report / status / reset` | `cmd_run`, `cmd_reset` |
| `config.py` | Loads YAML, resolves paths against the project root, validates values (bad regex, negative threshold or unknown URL fails fast) | `load_settings` |
| `discover.py` | Walks `data/landing/<arrival-date>/` in arrival order, parses each filename into store, date and version; supports `--until` | `discover_files`, `parse_filename` |
| `fingerprint.py` | SHA-256 of file contents, so identity is the bytes, not the name | `sha256_file` |
| `extract.py` | Reads the header with `csv`, then loads the body into DuckDB as raw text with row numbers | `read_header`, `load_raw` |
| `schema.py` | Normalises headers, maps aliases, detects extra or missing columns, fuzzy-maps renamed columns, computes layout signatures | `map_headers`, `layout_signature` |
| `transform.py` | Standardisation rules as tiny functions returning DuckDB SQL expressions, plus the query that applies them all | `parse_money`, `parse_datetime`, `normalise_store_code`, `standardise_select` |
| `validate.py` | The 9 data quality rules as one SQL `CASE` (first failure wins), plus the duplicate-key pass and readable detail messages | `validate`, `rule_case_sql` |
| `load.py` | Upserts dimensions, the file registry, the **atomic partition replace**, quarantine handling, Parquet output | `load_partition`, `quarantine_file`, `write_silver` |
| `warehouse.py` | One interface over SQLite and PostgreSQL: placeholders, transactions, bulk insert (`executemany` vs `COPY`) | `Warehouse` |
| `pipeline.py` | Orchestrates a run: gates (zero-byte, duplicate, stale version) → extract → map → standardise → validate → quarantine or load; counts everything; writes the run log | `Pipeline.run`, `Pipeline._process` |
| `alerts.py` | Finds runs of ≥ 2 consecutive missing store-days, upserts alerts, resolves ones filled by late files | `find_missing_runs`, `update_missing_day_alerts` |
| `reconcile.py` | Independent re-read of every source file; per-day reconciliation; data quality CSVs, `run_summary.json`, `DATA_QUALITY_SUMMARY.md` | `reconcile`, `write_reports` |
| `logging_utils.py` | JSON-lines file logger plus a readable console logger | `setup_logging`, `event` |

### Database
| File | Purpose |
|---|---|
| `sql/schema_sqlite.sql` | Tables, constraints (`UNIQUE`, `CHECK`, foreign keys), indexes and reporting views for SQLite |
| `sql/schema_postgres.sql` | The same model for PostgreSQL, with `NUMERIC`, `DATE`, `TIMESTAMPTZ`, `JSONB` and identity columns |

### Data
| Path | Purpose |
|---|---|
| `generator/generate_data.py` | Builds masters, daily files, injected defects, file events and the answer key. Deterministic by seed; standard library only |
| `data/master/` | Store and product master (committed) |
| `data/landing/` | Generated input (not committed; regenerate in about 35 s) |
| `data/_ground_truth/` | Generated answer key, used only by tests |
| `data/silver/` | Clean Parquet written by the pipeline |

### Analytics and BI
| Path | Purpose |
|---|---|
| `analytics/dbt_project.yml`, `profiles.yml` | dbt project and connection (DuckDB file `warehouse/analytics.duckdb`) |
| `analytics/models/sources.yml` | Points dbt at the Parquet files and master CSVs |
| `analytics/models/staging/*.sql` | Typed, renamed views over the sources |
| `analytics/models/marts/*.sql` | Business tables: daily store and category sales, store scorecard, payment mix, top products |
| `analytics/models/schema.yml` | Generic data tests (unique, not null, relationships, accepted values) |
| `analytics/tests/*.sql` | Custom data tests (amounts balance, price ≤ MRP, returns negative, totals complete) |
| `dashboard/app.py` | Streamlit dashboard: Sales, Stores and Data-quality tabs |

### Tests, CI and outputs
| Path | Purpose |
|---|---|
| `tests/conftest.py` | Shared fixtures: settings, DuckDB, a helper that pushes hand-written rows through the real code, sample-data generation |
| `tests/test_*.py` | 116 tests (see the README, §13) |
| `.github/workflows/ci.yml` | CI on Python 3.12 and 3.13 with a PostgreSQL service |
| `Makefile` | Shortcuts (`make data`, `make rerun`, `make daily`, `make dbt`, `make test`) |
| `reports/` | Results of the reference run (committed as evidence) |
| `docs/sample_run_log.jsonl` | Excerpt of a real JSON run log |

## 6. Tools used, and why

| Tool | Role here | Why this one | Alternatives considered |
|---|---|---|---|
| **Python** | Orchestration, CLI, registry logic | Required by the brief; it is the glue language of data engineering | – |
| **DuckDB** | Parsing, standardising and validating each file in SQL | Embedded (no server), very fast on CSV and Parquet, and expresses rules as readable SQL that runs on whole files at once | pandas (fine, but slower and type-guessing is risky); PySpark (built for clusters; too much overhead at 2M rows) |
| **SQLite** | Default warehouse | Zero setup; a single file; the brief allows it | – |
| **PostgreSQL** | Production warehouse option | Real decimals, concurrent writers, fast `COPY`, `JSONB` | MySQL (not supported by design) |
| **Parquet** | Silver layer | Columnar and compressed: ~170 MB of CSV became 27 MB. The lingua franca of data lakes; hive partitions allow per-partition overwrite | CSV (no types), plain DuckDB tables (less portable) |
| **dbt** | Analytics models and data tests | Industry standard for tested, version-controlled SQL transformations; widely asked for in analytics engineering roles | Hand-written SQL scripts (no tests or lineage) |
| **Streamlit** | Dashboard | A BI front end in about 150 lines of Python | Metabase or Superset (separate servers), Power BI (not code) |
| **pytest** | Tests | Fixtures and parametrisation make 116 tests cheap to write | unittest |
| **GitHub Actions** | CI | Free for public repos; can start a PostgreSQL container | – |
| **YAML** | Config | Human-editable; supports comments | JSON (no comments), env vars only (too flat) |

**Why not Spark or Hadoop?** Both are free, but they are built for data that doesn't fit on one machine. At 2M rows (~170 MB) a single laptop processes everything in 2.5 minutes, and Spark's start-up and coordination overhead would make it *slower*. Using a big-data tool on small data is a common over-engineering mistake. The honest answer is in the scaling plan: switch when data outgrows one machine.

## 7. Key design decisions and trade-offs

### 7.1 Idempotency through partition replace

Two ideas work together:

1. **Know what you've seen.** Every file's SHA-256 checksum is recorded. A file with the same path and checksum is skipped. The same bytes for the same store-day under a different name is a re-delivery and is skipped as a duplicate.
2. **Never append, always replace.** Data is never "added" to a store-day. The pipeline *replaces* the whole store-day in one transaction: delete, insert, record ownership, commit. Replaying the same file gives the same result, a corrected `_v2` file overwrites the old one cleanly, and a crash halfway rolls back to the previous good state.

The trade-off: if one bill in a store-day changes, the whole store-day (about 850 rows) is rewritten. At this size that costs milliseconds and buys simplicity.

### 7.2 Read everything as text first

The first read keeps every value as raw text. Conversion happens afterwards, and a failed conversion becomes NULL, which a rule then turns into a reason code. Letting a CSV reader guess types would mean `007` silently becomes `7`, or a bad date crashes the load. This is the **schema-on-read** idea.

### 7.3 Rules as SQL, first failure wins

All rules live in one ordered SQL `CASE` expression. Each row gets exactly one primary reason, which keeps the reject counts clean and comparable. The order matters: checking "is it a duplicate copy?" before "is the quantity valid?" means a copied bad row is labelled `DUPLICATE_ROW`, which matches how a human would describe it.

### 7.4 Keep the evidence

Rejected rows keep their row number, a readable detail and the full original record as JSON. Anyone can open the rejects table and see exactly what was wrong, and nothing is ever thrown away.

### 7.5 Quarantine, don't drop

A file from an unknown store is quarantined: recorded, with its rows kept as rejects. It is not ignored. Once the store is added to the master, `--retry-quarantined` loads it. Likewise, a file with more than 20% bad rows is held back entirely, because when that much is wrong the remaining "good" rows probably aren't trustworthy either.

### 7.6 Reconcile with a different code path

Reconciliation re-reads the source files with Python's `csv` module, not with DuckDB. If both paths shared a bug, comparing them would prove nothing. Independent checks are what make reconciliation meaningful.

### 7.7 Test against an answer key, not against yourself

The generator knows the truth because it created the problems. Tests compare the pipeline with that truth, not with numbers the pipeline itself produced.

## 8. Bugs found along the way

These were real bugs, caught by the tests. They are the most instructive part of the project.

| # | Bug | How it showed up | Root cause | Fix |
|---|---|---|---|---|
| 1 | Two different stores' header-only files were treated as **duplicates** of each other | E2E test: 4 duplicates counted instead of 3 | Two empty files with the same layout are byte-identical, so their checksums match | A duplicate must match on **checksum and store-day** ([pipeline.py](etl/pipeline.py)) |
| 2 | Retrying a quarantined file **skipped it as a duplicate of itself** | E2E test `test_retry_quarantined_after_adding_store` | The duplicate query found the file's own registry row | Exclude the file's own path from the duplicate check |
| 3 | Run statistics **counted rows from replaced v1 files** | Run summary said 2,025,652 loaded; the warehouse held 2,021,800 | The v1 rows were counted, then deleted when v2 replaced them | Subtract a replaced file's counts when it was loaded in the same run |
| 4 | The report said the "last run took 0.48 s" | After the no-op second run, the 10-minute check used that run's time | "Last run" is not the same as "last full run" | The performance check now uses the latest run that processed every file |
| 5 | A quarantined file's valid rows **broke a NOT NULL constraint** | Found while writing quarantine logic | Valid rows have no reason code, but rejects require one | Label them `REJECT_RATE_EXCEEDED` |

The lesson from #1 and #2: **identity is subtle.** "Same file" can mean same name, same bytes, or same business meaning, and getting it wrong in either direction is a data bug.

## 9. Results

| Check | Result |
|---|---|
| Rows loaded | 2,021,800, exactly the answer key |
| Rows rejected | 25,469 (1.244%), every reason code count equal to the answer key |
| Store-day totals | 2,364/2,364 equal to the answer key and within 0.5% reconciliation (difference $0.00) |
| Money trail | source $39,620,192.18 − rejected $421,145.17 = warehouse $39,199,047.01 |
| Alerts | the 3 expected; 7 interim alerts auto-resolved in the daily simulation |
| Full rerun | 152 s (SQLite), 154 s (PostgreSQL) |
| Second run | 0 files, 0 rows, 0.5 s |
| 62 daily runs | 2.6 s each on average; final state identical to one full run |
| Tests | 116 passing (unit, end-to-end, PostgreSQL, dashboard), 85% coverage |
| dbt | 8 models, 24 data tests, all pass; mart totals equal the warehouse |
| Storage | ~170 MB CSV → 27 MB Parquet |

## 10. What the data says (business view)

> ⚠️ The data is **synthetic**, so these patterns reflect the generator's assumptions, not real shoppers. They are shown to demonstrate what the analytics layer can answer.

| Question | Answer from the marts |
|---|---|
| Who sells the most? | 20 Supermarkets $19.2M, 8 Hypermarkets $14.1M, 12 Express $5.9M. A hypermarket averages about $29.7k/day against $8.4k for an Express store |
| Weekends? | Saturday and Sunday average about $19.5k per store-day vs about $15.1k Monday–Thursday (+29%) |
| Festive season? | The last 12 days averaged $17.9k per store-day vs $16.3k before (+10%) |
| Basket size? | About $57 per bill in every format: format drives the number of bills, not basket size |
| Returns and discounts? | Returns about 0.5% of sales value; discounts about 3.8% of gross |
| How do people pay? | UPI ~45%, cash ~28%, card ~22%, wallet ~5% of bills |
| Category mix | Home & Kitchen leads by value (55%) because it holds the high-ticket items (up to $249.99). In a real grocery chain Staples would dominate, so this is a generator artefact worth flagging, not a business finding |
| Data quality by store | `GGN041` is 100% rejected (not in the master); the next worst is ~1.3%. No store has a systemic problem |

Spotting that the category mix is an artefact of the generator matters in its own right: analysis should always be sanity-checked against domain knowledge.

## 11. Lessons learnt

**Data engineering concepts**
1. **Idempotency is a design property, not a feature you add.** Choosing "replace the partition" over "append rows" solved reruns, corrections and crash recovery all at once.
2. **Transactions are the safety net.** Without them, a crash midway would leave half a store-day loaded.
3. **Identity must be defined deliberately** (§8, bugs 1 and 2).
4. **Schema-on-read:** read raw, convert explicitly, reject with a reason. Never let a library guess types on dirty data.
5. **Configuration over code:** new stores, layouts, formats and thresholds are YAML changes.
6. **Observability:** structured JSON logs, a run table, a file registry and reports mean every question ("why is store 23 low on the 24th?") has an answer in data.
7. **Dates are dangerous:** `01/09` means different things in different countries. Be explicit per source, never guess per row.
8. **Business rules hide in "cosmetic" changes:** switching currency meant changing a validation tolerance.

**Testing**
9. **Build the answer key with the data.** A generator that knows the truth makes "is it correct?" a yes/no test.
10. **End-to-end tests find the bugs unit tests can't.** Every bug in §8 was an interaction between components.
11. **Test the unhappy paths:** crash mid-load, stale versions, a missing column, retries.
12. **Independent verification:** reconciliation and dbt each recomputed the totals another way and agreed to the cent.

**Tools and engineering judgement**
13. **Right-size the tools.** DuckDB on one laptop beats a cluster at 2M rows. Know when you'd switch (§12 and the README, §15).
14. **SQL is still the best language for data rules**, especially vectorised over whole files.
15. **Abstract the database early:** one small class made PostgreSQL support almost free.
16. **Layered architecture** (bronze CSV → silver Parquet → gold dbt marts) keeps raw, clean and business-ready data separate.

## 12. Limitations and next steps

| Limitation | Next step |
|---|---|
| Single-process; every run re-hashes every landing file | Process pool per partition; skip unchanged files by `(path, size, mtime)`; archive processed folders |
| SQLite allows one writer and stores money as `REAL` | PostgreSQL in production (already supported) |
| No scheduler | cron, or an Airflow/Dagster DAG with one task per arrival folder |
| Alerts only go to logs and a table | Email or Slack notification |
| The CLI module itself is untested (0% coverage) | A few `main([...])` tests |
| Synthetic data with simple patterns | Real POS samples; seasonality per category; price changes over time |
| Silver Parquet is written after the database commit, so a crash between the two leaves silver stale | Rebuild silver from the warehouse on demand, or write silver first to a temp path and rename atomically |

## 13. Talking about this project in an interview

**"Tell me about it in 30 seconds."**
> I built an ETL pipeline that ingests daily sales files from 40 stores, about 2 million rows in four inconsistent formats. It cleans and validates them with DuckDB, rejects bad rows with reason codes, and loads a SQLite or PostgreSQL warehouse idempotently. Each store-day is replaced in one transaction, so reruns and corrected files never double-count. I proved it against a generated answer key: an exact match, 0.0% reconciliation variance, 1.2% rejects, and a full rerun in 2.5 minutes. Then I added dbt models, a dashboard and CI.

**"How do you make it idempotent?"**
> Two things. A checksum registry, so I know which files I've seen. And partition replace: a store-day is always deleted and rewritten as a whole inside one transaction. Replaying the same file is a no-op, and a v2 replaces v1.

**"What was the hardest bug?"**
> Two stores sent empty files on different days. The files were byte-identical, so the second was flagged as a duplicate of the first. The fix was to define a duplicate as same bytes *for the same store-day*. Defining file identity carefully was a recurring theme.

**"Why not Spark?"**
> The data is ~170 MB; one machine handles it in minutes. Spark would add overhead with no benefit. I'd move to Spark, or to DuckDB on Parquet in a lake, once data no longer fits on one machine.

**"How do you know the numbers are right?"**
> Three independent checks agree to the cent. First, the generator's answer key. Second, reconciliation that re-reads every source file through a different code path. Third, the dbt marts recomputing totals from Parquet.

**"What if the chain doubles?"**
> Nothing in the code is store-specific; you add rows to the master. Daily runs stay small because they only process new files. For full reruns I'd parallelise by partition and move to PostgreSQL. The full answer is in the README, §15.

## 14. Glossary

| Term | Meaning |
|---|---|
| **ETL** | Extract, Transform, Load: read raw data, clean it, store it |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **Partition** | Here, all rows for one store on one business day |
| **Checksum (SHA-256)** | A fingerprint of a file's bytes; identical files have identical checksums |
| **Transaction** | A group of database changes that all succeed or all roll back |
| **Schema contract** | The agreed list of columns and types every input must map to |
| **Schema drift** | When a source changes its columns over time |
| **Quarantine** | Holding a suspicious file aside (recorded, not loaded) instead of dropping it |
| **Reconciliation** | Proving the warehouse totals match the source totals |
| **Bronze / silver / gold** | Raw → cleaned → business-ready data layers |
| **Parquet** | A compressed, columnar file format for analytics |
| **dbt** | A tool for building and testing SQL transformations as code |
| **MRP** | Maximum retail price, the highest price a product may legally be sold at |
| **Hive partitioning** | Folder names like `store_code=GGN001/business_date=2026-09-01` that encode column values |
