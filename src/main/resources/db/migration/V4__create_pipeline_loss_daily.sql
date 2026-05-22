-- ============================================================
-- V4__create_pipeline_loss_daily.sql
-- Pre-computed pipeline loss tracking for Insights Service
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_loss_daily (
    id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    summary_date      DATE            NOT NULL,
    source            VARCHAR(100)    NOT NULL,
    facility_id       VARCHAR(100),
    resource_type     VARCHAR(100),
    accepted_count    BIGINT          NOT NULL DEFAULT 0,
    matched_count     BIGINT          NOT NULL DEFAULT 0,
    unmatched_count   BIGINT          GENERATED ALWAYS AS (accepted_count - matched_count) STORED,
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_pipeline_loss_daily_composite
        UNIQUE (summary_date, source, facility_id, resource_type)
);

CREATE INDEX idx_pipeline_loss_daily_date      ON pipeline_loss_daily (summary_date);
CREATE INDEX idx_pipeline_loss_daily_unmatched ON pipeline_loss_daily ((accepted_count - matched_count)) WHERE (accepted_count - matched_count) > 0;
