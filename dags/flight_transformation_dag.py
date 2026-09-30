"""flight_transformation_pipeline

Hourly Airflow 3.x DAG: staging.flight_raw -> warehouse dims + fact_flight_hourly.

    check_flight_data_quality
            |
    +-------+--------+
    |                |
 load_dim_route  load_dim_airline
    |                |
    +-------+--------+
            |
   load_fact_flight_hourly

Uses CronDataIntervalTimetable explicitly so data_interval_start/end span a real hour.
(A plain cron string in Airflow 3.x defaults to CronTriggerTimetable, which gives
zero-width intervals.)
"""
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

CONN_ID = "weather_postgres"  # AIRFLOW_CONN_WEATHER_POSTGRES=postgresql://...
TZ = pendulum.timezone("Asia/Karachi")

# The hour being processed. Templated into every task.
INTERVAL_PARAMS = {
    "start": "{{ data_interval_start }}",
    "end": "{{ data_interval_end }}",
}

# "Event time" of a flight = whichever scheduled time exists (same trick as the dedup key)
EVENT_TIME = "COALESCE(scheduled_departure, scheduled_arrival)"


# ---------------------------------------------------------------- quality check
def check_flight_data_quality(data_interval_start, data_interval_end, **_):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    params = {"start": data_interval_start, "end": data_interval_end}

    total = hook.get_first(
        f"SELECT COUNT(*) FROM staging.flight_raw "
        f"WHERE {EVENT_TIME} >= %(start)s AND {EVENT_TIME} < %(end)s", parameters=params)[0]

    null_routes = hook.get_first(
        f"SELECT COUNT(*) FROM staging.flight_raw "
        f"WHERE {EVENT_TIME} >= %(start)s AND {EVENT_TIME} < %(end)s AND route_key IS NULL",
        parameters=params)[0]

    dupes = hook.get_first(
        f"""SELECT COUNT(*) FROM (
                SELECT 1 FROM staging.flight_raw
                WHERE {EVENT_TIME} >= %(start)s AND {EVENT_TIME} < %(end)s
                GROUP BY airline, flight_number, {EVENT_TIME}
                HAVING COUNT(*) > 1) d""", parameters=params)[0]

    print(f"[quality] rows={total} null_route_key={null_routes} duplicate_keys={dupes}")

    if dupes:
        raise AirflowFailException(f"{dupes} duplicate flight keys in this interval")
    if null_routes:
        raise AirflowFailException(f"{null_routes} rows without route_key (route filter failed?)")
    if total == 0:
        # Not a failure: the API budget may be exhausted or the hour may be quiet.
        print("[quality] WARNING: no flights in this interval")


# ---------------------------------------------------------------- SQL
SQL_DIM_ROUTE = """
INSERT INTO warehouse.dim_route (route_key, origin_iata, destination_iata)
SELECT DISTINCT route_key, origin_iata, destination_iata
FROM staging.flight_raw
WHERE route_key IS NOT NULL
ON CONFLICT (route_key) DO NOTHING;
"""

SQL_DIM_AIRLINE = """
INSERT INTO warehouse.dim_airline (airline)
SELECT DISTINCT airline FROM staging.flight_raw
WHERE airline IS NOT NULL
ON CONFLICT (airline) DO NOTHING;
"""

# risk_score (0-100) = 60 * cancel_rate + 30 * delay_rate + 10 * min(avg_delay / 120, 1)
# (rates are 0-1, so the maximum is exactly 100). The weights are a judgment call: tune them.
SQL_FACT = f"""
INSERT INTO warehouse.fact_flight_hourly
    (route_key, airline, hour_start, n_flights, n_delayed, n_cancelled,
     avg_delay_min, max_delay_min, delay_rate, cancel_rate, risk_score, updated_at)
SELECT
    route_key,
    airline,
    %(start)s::timestamptz                                        AS hour_start,
    COUNT(*)                                                      AS n_flights,
    COUNT(*) FILTER (WHERE COALESCE(delay_minutes, 0) >= 15)      AS n_delayed,
    COUNT(*) FILTER (WHERE LOWER(status) IN ('canceled','cancelled')) AS n_cancelled,
    ROUND(AVG(delay_minutes), 2)                                  AS avg_delay_min,
    MAX(delay_minutes)                                            AS max_delay_min,
    ROUND(COUNT(*) FILTER (WHERE COALESCE(delay_minutes, 0) >= 15)::numeric / COUNT(*), 4),
    ROUND(COUNT(*) FILTER (WHERE LOWER(status) IN ('canceled','cancelled'))::numeric / COUNT(*), 4),
    ROUND(LEAST(100,
        60 * COUNT(*) FILTER (WHERE LOWER(status) IN ('canceled','cancelled'))::numeric / COUNT(*)
      + 30 * COUNT(*) FILTER (WHERE COALESCE(delay_minutes, 0) >= 15)::numeric / COUNT(*)
      + 10 * LEAST(COALESCE(AVG(delay_minutes), 0) / 120.0, 1)
    ) * 1.0, 1)                                                  AS risk_score,
    now()
FROM staging.flight_raw
WHERE route_key IS NOT NULL
  AND {EVENT_TIME} >= %(start)s AND {EVENT_TIME} < %(end)s
GROUP BY route_key, airline
ON CONFLICT (route_key, airline, hour_start) DO UPDATE SET
    n_flights     = EXCLUDED.n_flights,
    n_delayed     = EXCLUDED.n_delayed,
    n_cancelled   = EXCLUDED.n_cancelled,
    avg_delay_min = EXCLUDED.avg_delay_min,
    max_delay_min = EXCLUDED.max_delay_min,
    delay_rate    = EXCLUDED.delay_rate,
    cancel_rate   = EXCLUDED.cancel_rate,
    risk_score    = EXCLUDED.risk_score,
    updated_at    = now();
"""

# ---------------------------------------------------------------- DAG
with DAG(
    dag_id="flight_transformation_pipeline",
    description="Hourly flight staging -> warehouse transformation",
    schedule=CronDataIntervalTimetable("0 * * * *", timezone=TZ),
    start_date=pendulum.datetime(2026, 8, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["flights", "warehouse"],
) as dag:

    quality = PythonOperator(
        task_id="check_flight_data_quality",
        python_callable=check_flight_data_quality,
    )

    dim_route = SQLExecuteQueryOperator(
        task_id="load_dim_route", conn_id=CONN_ID, sql=SQL_DIM_ROUTE)

    dim_airline = SQLExecuteQueryOperator(
        task_id="load_dim_airline", conn_id=CONN_ID, sql=SQL_DIM_AIRLINE)

    fact = SQLExecuteQueryOperator(
        task_id="load_fact_flight_hourly", conn_id=CONN_ID,
        sql=SQL_FACT, parameters=INTERVAL_PARAMS)

    quality >> [dim_route, dim_airline] >> fact
