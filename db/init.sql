CREATE TABLE metrics (
    id BIGSERIAL,
    ts TIMESTAMPTZ NOT NULL,
    host_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL
);
SELECT create_hypertable('metrics', 'ts');
CREATE INDEX idx_metrics_host_metric ON metrics (host_id, metric_name, ts DESC);

CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    host_id TEXT NOT NULL,
    metric_name TEXT,
    value DOUBLE PRECISION,
    score DOUBLE PRECISION,
    reason TEXT,
    source TEXT  -- 'rule' hoặc 'model'
);