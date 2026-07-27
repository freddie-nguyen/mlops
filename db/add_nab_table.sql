-- db/add_nab_table.sql
CREATE TABLE metrics_labeled (
    id BIGSERIAL,
    ts TIMESTAMPTZ NOT NULL,
    source_file TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX idx_metrics_labeled_file ON metrics_labeled (source_file, ts);