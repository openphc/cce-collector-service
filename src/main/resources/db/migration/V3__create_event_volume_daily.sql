-- ============================================================
-- V3__create_event_volume_daily.sql
-- Pre-computed daily event volume for Insights Service
-- ============================================================

CREATE TABLE IF NOT EXISTS event_volume_daily (
    id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    summary_date      DATE            NOT NULL,
    source            VARCHAR(100)    NOT NULL,
    facility_id       VARCHAR(100),
    resource_type     VARCHAR(100)    NOT NULL,
    event_count       BIGINT          NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT uq_event_volume_daily_composite
        UNIQUE (summary_date, source, facility_id, resource_type)
);

CREATE INDEX idx_event_volume_daily_date     ON event_volume_daily (summary_date);
CREATE INDEX idx_event_volume_daily_facility ON event_volume_daily (facility_id);
CREATE INDEX idx_event_volume_daily_source   ON event_volume_daily (source);
