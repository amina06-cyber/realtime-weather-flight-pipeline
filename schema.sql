CREATE SCHEMA IF NOT EXISTS staging;

-- one row per city per observation time (live 15-min or backfill hourly)
CREATE TABLE IF NOT EXISTS staging.weather_raw (
    id               BIGSERIAL PRIMARY KEY,
    city             TEXT        NOT NULL,
    observed_at      TIMESTAMPTZ NOT NULL,
    temperature_c    NUMERIC(5,2),
    humidity_pct     NUMERIC(5,2),
    wind_speed_kmh   NUMERIC(6,2),
    precipitation_mm NUMERIC(6,2),
    pressure_hpa     NUMERIC(7,2),
    source           TEXT        NOT NULL CHECK (source IN ('live','backfill')),
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (city, observed_at)
);
CREATE INDEX IF NOT EXISTS idx_weather_raw_observed ON staging.weather_raw (observed_at);

-- 5-minute tumbling-window aggregates produced by the stream processor
CREATE TABLE IF NOT EXISTS staging.weather_aggregates (
    city             TEXT        NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    n_readings       INT         NOT NULL,
    avg_temp_c       NUMERIC(5,2),
    min_temp_c       NUMERIC(5,2),
    max_temp_c       NUMERIC(5,2),
    avg_humidity_pct NUMERIC(5,2),
    max_wind_kmh     NUMERIC(6,2),
    total_precip_mm  NUMERIC(6,2),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, window_start)
);

-- threshold alerts
CREATE TABLE IF NOT EXISTS staging.weather_alerts (
    id           BIGSERIAL PRIMARY KEY,
    city         TEXT        NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    alert_type   TEXT        NOT NULL,
    metric_value NUMERIC(8,2) NOT NULL,
    threshold    NUMERIC(8,2) NOT NULL,
    message      TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (city, observed_at, alert_type)
);
