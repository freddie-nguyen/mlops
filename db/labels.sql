CREATE TABLE labeled_events (
    id BIGSERIAL PRIMARY KEY,
    host_id TEXT NOT NULL,
    metric_group TEXT NOT NULL,        -- 'cpu', 'mem', 'disk', 'net'
    label TEXT NOT NULL,               -- 'anomaly' hoặc 'normal'
    scenario TEXT,                     -- vd 'stress-ng-cpu-100pct', 'disk-fill', 'network-flood'
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    notes TEXT
);