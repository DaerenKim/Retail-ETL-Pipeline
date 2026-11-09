-- Retail ETL warehouse - SQLite DDL.  Run with:  python -m etl init-db
-- Money is stored as REAL rounded to 2 dp (SQLite has no DECIMAL); see schema_postgres.sql for NUMERIC.
PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dim_store (
    store_code   TEXT PRIMARY KEY,
    store_name   TEXT NOT NULL,
    locality     TEXT,
    zone         TEXT,
    city         TEXT,
    state        TEXT,
    format       TEXT,
    pos_system   TEXT,
    open_date    TEXT,
    sqft         INTEGER,
    is_active    TEXT NOT NULL DEFAULT 'Y' CHECK (is_active IN ('Y', 'N')),
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_code TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category     TEXT,
    brand        TEXT,
    pack_size    TEXT,
    mrp          REAL NOT NULL CHECK (mrp > 0),
    gst_rate     INTEGER,
    is_active    TEXT NOT NULL DEFAULT 'Y' CHECK (is_active IN ('Y', 'N')),
    updated_at   TEXT NOT NULL
);

-- ---------------------------------------------------------------- ETL control tables
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id            TEXT PRIMARY KEY,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL,              -- RUNNING / SUCCESS / SUCCESS_WITH_WARNINGS / FAILED
    until_date        TEXT,
    files_seen        INTEGER DEFAULT 0,
    files_new         INTEGER DEFAULT 0,
    files_loaded      INTEGER DEFAULT 0,
    files_skipped     INTEGER DEFAULT 0,
    files_quarantined INTEGER DEFAULT 0,
    files_failed      INTEGER DEFAULT 0,
    rows_read         INTEGER DEFAULT 0,
    rows_loaded       INTEGER DEFAULT 0,
    rows_rejected     INTEGER DEFAULT 0,
    duration_s        REAL,
    log_file          TEXT
);

-- One row per distinct (path, checksum) ever seen. This is the processed-file registry.
CREATE TABLE IF NOT EXISTS etl_file_log (
    file_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    file_name         TEXT NOT NULL,
    landing_folder    TEXT NOT NULL,
    file_path         TEXT NOT NULL,
    store_code        TEXT,
    business_date     TEXT,
    file_version      INTEGER,
    sha256            TEXT NOT NULL,
    bytes             INTEGER,
    layout_signature  TEXT,
    column_mapping    TEXT,                       -- JSON: canonical column -> source header
    rows_read         INTEGER DEFAULT 0,
    rows_loaded       INTEGER DEFAULT 0,
    rows_rejected     INTEGER DEFAULT 0,
    source_net_amount REAL,
    loaded_net_amount REAL,
    status            TEXT NOT NULL,              -- LOADED / SUPERSEDED / EMPTY / QUARANTINED / FAILED / SKIPPED_DUPLICATE / SKIPPED_STALE_VERSION
    reason_code       TEXT,
    message           TEXT,
    processed_at      TEXT NOT NULL,
    UNIQUE (file_path, sha256)
);
CREATE INDEX IF NOT EXISTS ix_file_log_sha ON etl_file_log (sha256);
CREATE INDEX IF NOT EXISTS ix_file_log_partition ON etl_file_log (store_code, business_date);

-- Which file currently owns each (store, business_date) partition.
CREATE TABLE IF NOT EXISTS etl_partition (
    store_code    TEXT NOT NULL,
    business_date TEXT NOT NULL,
    file_id       INTEGER NOT NULL REFERENCES etl_file_log (file_id),
    file_version  INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    rows_loaded   INTEGER NOT NULL,
    net_amount    REAL NOT NULL,
    loaded_at     TEXT NOT NULL,
    PRIMARY KEY (store_code, business_date)
);

-- ---------------------------------------------------------------- facts
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id      INTEGER PRIMARY KEY,
    store_code    TEXT NOT NULL REFERENCES dim_store (store_code),
    business_date TEXT NOT NULL,
    bill_no       TEXT NOT NULL,
    bill_datetime TEXT NOT NULL,
    product_code  TEXT NOT NULL REFERENCES dim_product (product_code),
    quantity      INTEGER NOT NULL CHECK (quantity <> 0),
    unit_price    REAL NOT NULL CHECK (unit_price > 0),
    discount      REAL NOT NULL DEFAULT 0 CHECK (discount >= 0),
    net_amount    REAL NOT NULL,
    payment_mode  TEXT NOT NULL,
    is_return     INTEGER NOT NULL CHECK (is_return IN (0, 1)),
    file_id       INTEGER NOT NULL REFERENCES etl_file_log (file_id),
    UNIQUE (store_code, bill_no, product_code)
);
CREATE INDEX IF NOT EXISTS ix_fact_partition ON fact_sales (store_code, business_date);
CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_sales (business_date);
CREATE INDEX IF NOT EXISTS ix_fact_product ON fact_sales (product_code);

CREATE TABLE IF NOT EXISTS etl_rejected_rows (
    reject_id     INTEGER PRIMARY KEY,
    file_id       INTEGER NOT NULL REFERENCES etl_file_log (file_id),
    run_id        TEXT NOT NULL,
    store_code    TEXT NOT NULL,                  -- store/date of the FILE (partition key)
    business_date TEXT NOT NULL,
    row_number    INTEGER NOT NULL,               -- 1-based data row in the source file
    reason_code   TEXT NOT NULL,
    reason_detail TEXT,
    net_amount    REAL,                           -- parsed amount when parseable (for reconciliation)
    raw_record    TEXT NOT NULL                   -- JSON of the original row
);
CREATE INDEX IF NOT EXISTS ix_reject_partition ON etl_rejected_rows (store_code, business_date);
CREATE INDEX IF NOT EXISTS ix_reject_reason ON etl_rejected_rows (reason_code);
CREATE INDEX IF NOT EXISTS ix_reject_file ON etl_rejected_rows (file_id);

CREATE TABLE IF NOT EXISTS etl_alerts (
    alert_id         INTEGER PRIMARY KEY,
    alert_type       TEXT NOT NULL,               -- MISSING_CONSECUTIVE_DAYS
    store_code       TEXT NOT NULL,
    first_date       TEXT NOT NULL,
    last_date        TEXT NOT NULL,
    consecutive_days INTEGER NOT NULL,
    details          TEXT,
    raised_at        TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    UNIQUE (alert_type, store_code, first_date)
);

-- ---------------------------------------------------------------- reporting views
DROP VIEW IF EXISTS v_daily_store_sales;
CREATE VIEW v_daily_store_sales AS
SELECT store_code,
       business_date,
       COUNT(*)                                             AS lines,
       COUNT(DISTINCT bill_no)                              AS bills,
       SUM(quantity)                                        AS units,
       ROUND(SUM(quantity * unit_price), 2)                 AS gross_amount,
       ROUND(SUM(discount), 2)                              AS discount,
       ROUND(SUM(net_amount), 2)                            AS net_amount,
       ROUND(SUM(CASE WHEN is_return = 1 THEN net_amount ELSE 0 END), 2) AS returns_net
FROM fact_sales
GROUP BY store_code, business_date;

DROP VIEW IF EXISTS v_dq_summary;
CREATE VIEW v_dq_summary AS
SELECT reason_code, COUNT(*) AS rejected_rows, COUNT(DISTINCT file_id) AS files_affected
FROM etl_rejected_rows
GROUP BY reason_code;

DROP VIEW IF EXISTS v_file_status;
CREATE VIEW v_file_status AS
SELECT status, COALESCE(reason_code, '') AS reason_code, COUNT(*) AS files, SUM(rows_read) AS rows_read
FROM etl_file_log
GROUP BY status, COALESCE(reason_code, '');
