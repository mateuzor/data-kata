-- ============================================================
-- OUTPUT DATABASE - Aggregated Results Schema
-- ============================================================

-- ─────────────────────────────────────────────
-- Pipeline A: Top Sales per City
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS top_sales_per_city (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    state           CHAR(2)      NOT NULL,
    total_amount    NUMERIC(18,2) NOT NULL,
    total_orders    INT           NOT NULL,
    rank_position   INT           NOT NULL,
    period_start    DATE          NOT NULL,
    period_end      DATE          NOT NULL,
    pipeline        VARCHAR(50)   DEFAULT 'top_sales_per_city',
    processed_at    TIMESTAMP     DEFAULT NOW(),
    UNIQUE (city, state, period_start, period_end)
);

CREATE INDEX idx_top_city_rank   ON top_sales_per_city(rank_position);
CREATE INDEX idx_top_city_period ON top_sales_per_city(period_start, period_end);

-- ─────────────────────────────────────────────
-- Pipeline B: Top Salesman
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS top_salesman (
    id              SERIAL PRIMARY KEY,
    salesman_id     INT           NOT NULL,
    salesman_name   VARCHAR(100)  NOT NULL,
    city            VARCHAR(100)  NOT NULL,
    state           CHAR(2)       NOT NULL,
    region          VARCHAR(50)   NOT NULL,
    total_amount    NUMERIC(18,2) NOT NULL,
    total_orders    INT           NOT NULL,
    rank_position   INT           NOT NULL,
    period_start    DATE          NOT NULL,
    period_end      DATE          NOT NULL,
    pipeline        VARCHAR(50)   DEFAULT 'top_salesman',
    processed_at    TIMESTAMP     DEFAULT NOW(),
    UNIQUE (salesman_id, period_start, period_end)
);

CREATE INDEX idx_top_salesman_rank   ON top_salesman(rank_position);
CREATE INDEX idx_top_salesman_period ON top_salesman(period_start, period_end);

-- ─────────────────────────────────────────────
-- Pipeline lineage metadata
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100)  NOT NULL,
    status          VARCHAR(20)   NOT NULL,  -- running | success | failed
    started_at      TIMESTAMP     NOT NULL,
    finished_at     TIMESTAMP,
    rows_processed  INT,
    error_message   TEXT,
    run_id          UUID          DEFAULT gen_random_uuid()
);
