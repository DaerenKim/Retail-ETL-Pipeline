-- Retail ETL warehouse - PostgreSQL DDL.
-- Used automatically when warehouse.url (or RETAIL_WAREHOUSE_URL) is postgresql://...
-- Run with:  RETAIL_WAREHOUSE_URL=postgresql://user@localhost:5432/retail python -m etl init-db

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
    open_date    DATE,
    sqft         INTEGER,
    is_active    CHAR(1) NOT NULL DEFAULT 'Y' CHECK (is_active IN ('Y', 'N')),
    updated_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_code TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category     TEXT,
    brand        TEXT,
    pack_size    TEXT,
    mrp          NUMERIC(12, 2) NOT NULL CHECK (mrp > 0),
    gst_rate     INTEGER,
    is_active    CHAR(1) NOT NULL DEFAULT 'Y' CHECK (is_active IN ('Y', 'N')),
    updated_at   TIMESTAMPTZ NOT NULL
);

-- ---------------------------------------------------------------- ETL control tables
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id            TEXT PRIMARY KEY,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    status            TEXT NOT NULL,
    until_date        DATE,
    files_seen        INTEGER DEFAULT 0,
    files_new         INTEGER DEFAULT 0,
    files_loaded      INTEGER DEFAULT 0,
    files_skipped     INTEGER DEFAULT 0,
    files_quarantined INTEGER DEFAULT 0,
    files_failed      INTEGER DEFAULT 0,
    rows_read         INTEGER DEFAULT 0,
    rows_loaded       INTEGER DEFAULT 0,
    rows_rejected     INTEGER DEFAULT 0,
    duration_s        DOUBLE PRECISION,
    log_file          TEXT
);

CREATE TABLE IF NOT EXISTS etl_file_log (
    file_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            TEXT NOT NULL,
    file_name         TEXT NOT NULL,
    landing_folder    TEXT NOT NULL,
    file_path         TEXT NOT NULL,
    store_code        TEXT,
    business_date     DATE,
    file_version      INTEGER,
    sha256            CHAR(64) NOT NULL,
    bytes             BIGINT,
    layout_signature  TEXT,
    column_mapping    JSONB,
    rows_read         INTEGER DEFAULT 0,
    rows_loaded       INTEGER DEFAULT 0,
    rows_rejected     INTEGER DEFAULT 0,
    source_net_amount NUMERIC(16, 2),
    loaded_net_amount NUMERIC(16, 2),
    status            TEXT NOT NULL,
    reason_code       TEXT,
    message           TEXT,
    processed_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (file_path, sha256)
);
CREATE INDEX IF NOT EXISTS ix_file_log_sha ON etl_file_log (sha256);
CREATE INDEX IF NOT EXISTS ix_file_log_partition ON etl_file_log (store_code, business_date);

CREATE TABLE IF NOT EXISTS etl_partition (
    store_code    TEXT NOT NULL,
    business_date DATE NOT NULL,
    file_id       BIGINT NOT NULL REFERENCES etl_file_log (file_id),
    file_version  INTEGER NOT NULL,
    sha256        CHAR(64) NOT NULL,
    rows_loaded   INTEGER NOT NULL,
    net_amount    NUMERIC(16, 2) NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (store_code, business_date)
);

-- ---------------------------------------------------------------- facts
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_code    TEXT NOT NULL REFERENCES dim_store (store_code),
    business_date DATE NOT NULL,
    bill_no       TEXT NOT NULL,
    bill_datetime TIMESTAMP NOT NULL,
    product_code  TEXT NOT NULL REFERENCES dim_product (product_code),
    quantity      INTEGER NOT NULL CHECK (quantity <> 0),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    discount      NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (discount >= 0),
    net_amount    NUMERIC(14, 2) NOT NULL,
    payment_mode  TEXT NOT NULL,
    is_return     SMALLINT NOT NULL CHECK (is_return IN (0, 1)),
    file_id       BIGINT NOT NULL REFERENCES etl_file_log (file_id),
    UNIQUE (store_code, bill_no, product_code)
);
CREATE INDEX IF NOT EXISTS ix_fact_partition ON fact_sales (store_code, business_date);
CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_sales (business_date);
CREATE INDEX IF NOT EXISTS ix_fact_product ON fact_sales (product_code);

CREATE TABLE IF NOT EXISTS etl_rejected_rows (
    reject_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id       BIGINT NOT NULL REFERENCES etl_file_log (file_id),
    run_id        TEXT NOT NULL,
    store_code    TEXT NOT NULL,
    business_date DATE NOT NULL,
    row_number    INTEGER NOT NULL,
    reason_code   TEXT NOT NULL,
    reason_detail TEXT,
    net_amount    NUMERIC(16, 2),
    raw_record    JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_reject_partition ON etl_rejected_rows (store_code, business_date);
CREATE INDEX IF NOT EXISTS ix_reject_reason ON etl_rejected_rows (reason_code);
CREATE INDEX IF NOT EXISTS ix_reject_file ON etl_rejected_rows (file_id);

CREATE TABLE IF NOT EXISTS etl_alerts (
    alert_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_type       TEXT NOT NULL,
    store_code       TEXT NOT NULL,
    first_date       DATE NOT NULL,
    last_date        DATE NOT NULL,
    consecutive_days INTEGER NOT NULL,
    details          TEXT,
    raised_at        TIMESTAMPTZ NOT NULL,
    run_id           TEXT NOT NULL,
    UNIQUE (alert_type, store_code, first_date)
);

-- ---------------------------------------------------------------- reporting views
CREATE OR REPLACE VIEW v_daily_store_sales AS
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

CREATE OR REPLACE VIEW v_dq_summary AS
SELECT reason_code, COUNT(*) AS rejected_rows, COUNT(DISTINCT file_id) AS files_affected
FROM etl_rejected_rows
GROUP BY reason_code;

CREATE OR REPLACE VIEW v_file_status AS
SELECT status, COALESCE(reason_code, '') AS reason_code, COUNT(*) AS files, SUM(rows_read) AS rows_read
FROM etl_file_log
GROUP BY status, COALESCE(reason_code, '');
