# Real-Time Weather & Flight Pipeline 
A streaming data pipeline built around one idea: take a live, messy data source, push it through Kafka, clean it up in PostgreSQL, and end up with something a dashboard can actually use.

It currently handles two kinds of data:
- **Weather** for Pakistani cities, streamed from [Open-Meteo](https://open-meteo.com/) (free, no API key)
- **Flights** between a set of tracked routes, transformed into an hourly "risk" view per route and airline

I built it as a learning project, and I've tried to be honest in this README about what's finished and what isn't.

## How it fits together

```
WEATHER                                          FLIGHTS
Open-Meteo API                                   Flight API (AeroDataBox)
   │ live + backfill                                │
   ▼                                                ▼
Kafka: weather-raw                               staging.flight_raw
   │                                                │
   ▼                                                │
Stream processor (consumer.py)                      │
   ├─ raw readings   → staging.weather_raw          │
   ├─ 5-min windows  → staging.weather_aggregates   │
   └─ alerts         → staging.weather_alerts       │
   │                                                │
   ▼                                                ▼
insights.sql views                     Airflow DAG (hourly) → warehouse.*
   │                                                │
   └──────────────► Tableau dashboards ◄────────────┘
```

## What's in the repo
| File | What it's for |
|---|---|
| `common.py` | Shared settings, message schemas, Kafka and Postgres helpers |
| `producer.py` | Pulls weather from Open-Meteo (live or backfill) and publishes to Kafka |
| `consumer.py` | Reads from Kafka, builds 5-minute summaries, raises alerts, writes to Postgres |
| `schema.sql` | Weather staging tables |
| `insights.sql` | Views for Tableau (hourly, daily, alerts, city benchmarks, pipeline health) |
| `flight_schema.sql` | Flight staging and warehouse tables |
| `dags/flight_transformation_dag.py` | Hourly Airflow DAG that turns raw flights into a warehouse fact table |
| `docker-compose.yml` | Runs Kafka and PostgreSQL locally |
| `requirements.txt` | Python dependencies |

## Running the weather pipeline

You'll need Docker and Python 3.10+.
```bash
docker compose up -d                        # Kafka + Postgres (tables are created on first start)
pip install -r requirements.txt
python consumer.py                          # terminal 1, leave running
python producer.py backfill --days 30       # terminal 2, load some history
python producer.py live                     # then switch to live data
```

Cities, alert thresholds and the window size live at the top of `common.py`, so they're easy to change.

## The flight transformation
The DAG `flight_transformation_pipeline` runs every hour and does four things:
1. **Checks data quality**: fails the run if there are duplicate flights or flights without a route
2. **Loads `dim_route`** and **`dim_airline`**
3. **Builds `fact_flight_hourly`**: flights, delays (15+ minutes), cancellations, average delay, and a 0–100 risk score per route and airline

A few design choices worth knowing about:
- **It uses `CronDataIntervalTimetable`** on purpose. In Airflow 3, a plain cron string gives zero-width data intervals, which breaks anything that depends on a real hour.
- **Deduplication uses `COALESCE(scheduled_departure, scheduled_arrival)` plus airline and flight number.** Arrival rows have no departure time, and in Postgres `NULL` never equals `NULL`, so a simple unique constraint would let duplicates through.
- **The risk score is a simple heuristic**: 60% cancellations, 30% delays, 10% average delay size. The weights are a judgment call, not a validated model.

To run it you'll need Airflow 3.x with the Postgres provider, and a connection called `weather_postgres` pointing at this database.

## Where things stand

**Working (as far as I've run them):** the weather producer, consumer and views.
**Written but not fully tested:** the flight DAG and schema.
**Not in this repo yet:** the flight ingestion, meaning the producer and consumer that pull flight data into `staging.flight_raw`, and the Tableau workbooks. Until those exist, the flight DAG needs `flight_raw` to be filled some other way.

## Good to know
- Live weather updates about every 15 minutes, so a 5-minute window usually holds one reading. Quiet windows are closed after 30 seconds.
- Open-Meteo is free but has fair-use limits. The producer retries with a backoff if it's rate-limited.
- Writes are idempotent: if the consumer restarts and sees a message twice, you won't get duplicate rows.

## Tech
Python · Apache Kafka · PostgreSQL · Apache Airflow 3 · Docker Compose · Pydantic · Tableau

---

Questions or suggestions? Open an issue. 💬
