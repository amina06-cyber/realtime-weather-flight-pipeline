CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Raw flight events (one row per flight leg as seen at a polled airport)
CREATE TABLE IF NOT EXISTS staging.flight_raw (
    id                  BIGSERIAL PRIMARY KEY,
    flight_number       TEXT        NOT NULL,
    airline             TEXT        NOT NULL,
    direction           TEXT        NOT NULL CHECK (direction IN ('departure','arrival')),
    origin_iata         TEXT,
    destination_iata    TEXT,
    route_key           TEXT,                -- e.g. 'LHE-DXB'; NULL = untracked route
    scheduled_departure TIMESTAMPTZ,         -- NULL for arrival-direction rows
    scheduled_arrival   TIMESTAMPTZ,
    actual_departure    TIMESTAMPTZ,
    actual_arrival      TIMESTAMPTZ,
    status              TEXT,
    delay_minutes       INT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedup key: Postgres treats NULL <> NULL, so a plain UNIQUE on scheduled_departure
-- misses arrival rows. COALESCE picks whichever scheduled time exists, and airline is
-- included because flight numbers are not globally unique.
CREATE UNIQUE INDEX IF NOT EXISTS uq_flight_raw_dedup
    ON staging.flight_raw (airline, flight_number, (COALESCE(scheduled_departure, scheduled_arrival)));

-- Track usage against the provider's free-tier limits
CREATE TABLE IF NOT EXISTS staging.api_usage_tracker (
    usage_date     DATE        NOT NULL,
    provider       TEXT        NOT NULL,
    requests_made  INT         NOT NULL DEFAULT 0,
    units_used     INT         NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (usage_date, provider)
);

-- Warehouse layer
CREATE TABLE IF NOT EXISTS warehouse.dim_route (
    route_key         TEXT PRIMARY KEY,
    origin_iata       TEXT,
    destination_iata  TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_airline (
    airline TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS warehouse.fact_flight_hourly (
    route_key       TEXT        NOT NULL REFERENCES warehouse.dim_route (route_key),
    airline         TEXT        NOT NULL REFERENCES warehouse.dim_airline (airline),
    hour_start      TIMESTAMPTZ NOT NULL,
    n_flights       INT         NOT NULL,
    n_delayed       INT         NOT NULL,   -- delay >= 15 min
    n_cancelled     INT         NOT NULL,
    avg_delay_min   NUMERIC(7,2),
    max_delay_min   INT,
    delay_rate      NUMERIC(5,4),
    cancel_rate     NUMERIC(5,4),
    risk_score      NUMERIC(5,1),           -- 0-100, see DAG for the formula
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (route_key, airline, hour_start)
);
