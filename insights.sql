CREATE SCHEMA IF NOT EXISTS insights;

-- 1. Hourly trends per city (local Pakistan time)
CREATE OR REPLACE VIEW insights.v_tableau_hourly_master AS
SELECT
    city,
    date_trunc('hour', observed_at AT TIME ZONE 'Asia/Karachi') AS hour_local,
    ROUND(AVG(temperature_c), 2)    AS avg_temp_c,
    MIN(temperature_c)              AS min_temp_c,
    MAX(temperature_c)              AS max_temp_c,
    ROUND(AVG(humidity_pct), 2)     AS avg_humidity_pct,
    MAX(wind_speed_kmh)             AS max_wind_kmh,
    SUM(precipitation_mm)           AS total_precip_mm,
    ROUND(AVG(pressure_hpa), 2)     AS avg_pressure_hpa,
    COUNT(*)                        AS n_readings
FROM staging.weather_raw
GROUP BY city, date_trunc('hour', observed_at AT TIME ZONE 'Asia/Karachi');

-- 2. Daily climate summary
CREATE OR REPLACE VIEW insights.v_tableau_daily_climate AS
SELECT
    city,
    (observed_at AT TIME ZONE 'Asia/Karachi')::date AS day_local,
    ROUND(AVG(temperature_c), 2)  AS avg_temp_c,
    MIN(temperature_c)            AS min_temp_c,
    MAX(temperature_c)            AS max_temp_c,
    MAX(temperature_c) - MIN(temperature_c) AS temp_range_c,
    ROUND(AVG(humidity_pct), 2)   AS avg_humidity_pct,
    MAX(wind_speed_kmh)           AS max_wind_kmh,
    SUM(precipitation_mm)         AS total_precip_mm
FROM staging.weather_raw
GROUP BY city, (observed_at AT TIME ZONE 'Asia/Karachi')::date;

-- 3. Alert frequency
CREATE OR REPLACE VIEW insights.v_tableau_alert_analytics AS
SELECT
    city,
    alert_type,
    (observed_at AT TIME ZONE 'Asia/Karachi')::date AS day_local,
    COUNT(*)            AS alert_count,
    MAX(metric_value)   AS peak_value,
    MAX(threshold)      AS threshold
FROM staging.weather_alerts
GROUP BY city, alert_type, (observed_at AT TIME ZONE 'Asia/Karachi')::date;

-- 4. City benchmarking (vs. the all-city average, with rankings)
CREATE OR REPLACE VIEW insights.v_tableau_city_benchmarks AS
WITH per_city AS (
    SELECT city,
           AVG(temperature_c)   AS avg_temp_c,
           AVG(humidity_pct)    AS avg_humidity_pct,
           MAX(wind_speed_kmh)  AS max_wind_kmh,
           SUM(precipitation_mm) AS total_precip_mm
    FROM staging.weather_raw
    GROUP BY city
)
SELECT
    city,
    ROUND(avg_temp_c, 2)        AS avg_temp_c,
    ROUND(avg_humidity_pct, 2)  AS avg_humidity_pct,
    max_wind_kmh,
    total_precip_mm,
    ROUND(avg_temp_c - AVG(avg_temp_c) OVER (), 2) AS temp_vs_all_cities_c,
    RANK() OVER (ORDER BY avg_temp_c DESC)         AS hottest_rank,
    RANK() OVER (ORDER BY total_precip_mm DESC)    AS wettest_rank
FROM per_city;

-- 5. Pipeline health (freshness + volume per city and source)
CREATE OR REPLACE VIEW insights.v_tableau_pipeline_health AS
SELECT
    city,
    source,
    COUNT(*)                                   AS rows_total,
    MAX(observed_at)                           AS latest_observation,
    MAX(ingested_at)                           AS latest_ingest,
    ROUND(EXTRACT(EPOCH FROM (now() - MAX(ingested_at))) / 60.0, 1) AS minutes_since_ingest
FROM staging.weather_raw
GROUP BY city, source;
