-- ============================================================
-- V2__create_ingestion_summary_daily.sql
-- Pre-computed daily ingestion summary for Insights Service
-- ============================================================

CREATE TABLE IF NOT EXISTS ingestion_summary_daily (
    id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    summary_date      DATE            NOT NULL,
    source            VARCHAR(100)    NOT NULL,
    facility_id       VARCHAR(100),
    status            VARCHAR(20)     NOT NULL,
    rejection_reason  VARCHAR(50),
    resource_type     VARCHAR(100),
    event_count       BIGINT          NOT NULL DEFAULT 0,
    distinct_patients BIGINT          NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_ingestion_daily_composite
        UNIQUE (summary_date, source, facility_id, status, rejection_reason, resource_type)
);

CREATE INDEX idx_ingestion_daily_date   ON ingestion_summary_daily (summary_date);
CREATE INDEX idx_ingestion_daily_source ON ingestion_summary_daily (source);
CREATE INDEX idx_ingestion_daily_status ON ingestion_summary_daily (status);
