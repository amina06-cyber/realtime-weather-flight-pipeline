"""Consumer + stream processor.

Reads `weather-raw`, then:
  1. upserts raw readings into staging.weather_raw
  2. aggregates them in 5-minute tumbling windows (event time) -> weather-aggregates + table
  3. checks thresholds -> weather-alerts + table
Offsets are committed only after the DB writes succeed (at-least-once, idempotent upserts).
"""
import operator
import time
from collections import defaultdict
from datetime import datetime

from psycopg2.extras import execute_values

from common import (THRESHOLDS, TOPIC_AGG, TOPIC_ALERTS, TOPIC_RAW, TZ,
                    WINDOW_SECONDS, WeatherAggregate, WeatherAlert,
                    WeatherReading, get_conn, get_logger, make_consumer,
                    make_producer)

log = get_logger("consumer")
OPS = {">": operator.gt, "<": operator.lt}
IDLE_FLUSH_SECONDS = 30  # close open windows if the stream goes quiet


def _vals(rows, attr):
    return [getattr(r, attr) for r in rows if getattr(r, attr) is not None]


class WindowAggregator:
    def __init__(self):
        self.buckets = defaultdict(list)  # (city, window_start_epoch) -> [readings]
        self.latest = {}                  # city -> newest event-time epoch seen

    def add(self, r: WeatherReading):
        ts = int(r.observed_at.timestamp())
        start = ts - ts % WINDOW_SECONDS
        self.buckets[(r.city, start)].append(r)
        self.latest[r.city] = max(self.latest.get(r.city, 0), ts)

    def flush(self, force: bool = False) -> list[WeatherAggregate]:
        closed = []
        for (city, start) in list(self.buckets):
            end = start + WINDOW_SECONDS
            if force or end <= self.latest[city]:
                rows = self.buckets.pop((city, start))
                t, h = _vals(rows, "temperature_c"), _vals(rows, "humidity_pct")
                w, p = _vals(rows, "wind_speed_kmh"), _vals(rows, "precipitation_mm")
                closed.append(WeatherAggregate(
                    city=city,
                    window_start=datetime.fromtimestamp(start, TZ),
                    window_end=datetime.fromtimestamp(end, TZ),
                    n_readings=len(rows),
                    avg_temp_c=round(sum(t) / len(t), 2) if t else None,
                    min_temp_c=min(t) if t else None,
                    max_temp_c=max(t) if t else None,
                    avg_humidity_pct=round(sum(h) / len(h), 2) if h else None,
                    max_wind_kmh=max(w) if w else None,
                    total_precip_mm=round(sum(p), 2) if p else None))
        return closed


def check_alerts(r: WeatherReading) -> list[WeatherAlert]:
    alerts = []
    for metric, cmp_, limit, kind in THRESHOLDS:
        v = getattr(r, metric)
        if v is not None and OPS[cmp_](v, limit):
            alerts.append(WeatherAlert(
                city=r.city, observed_at=r.observed_at, alert_type=kind,
                metric_value=v, threshold=limit,
                message=f"{kind}: {metric}={v} {cmp_} {limit} in {r.city}"))
    return alerts


# ---------------------------------------------------------------- sinks
def save_raw(cur, rows: list[WeatherReading]):
    execute_values(cur, """
        INSERT INTO staging.weather_raw
          (city, observed_at, temperature_c, humidity_pct, wind_speed_kmh,
           precipitation_mm, pressure_hpa, source)
        VALUES %s
        ON CONFLICT (city, observed_at) DO UPDATE SET
          temperature_c=EXCLUDED.temperature_c, humidity_pct=EXCLUDED.humidity_pct,
          wind_speed_kmh=EXCLUDED.wind_speed_kmh, precipitation_mm=EXCLUDED.precipitation_mm,
          pressure_hpa=EXCLUDED.pressure_hpa, source=EXCLUDED.source""",
        [(r.city, r.observed_at, r.temperature_c, r.humidity_pct, r.wind_speed_kmh,
          r.precipitation_mm, r.pressure_hpa, r.source) for r in rows])


def save_aggs(cur, rows: list[WeatherAggregate]):
    execute_values(cur, """
        INSERT INTO staging.weather_aggregates
          (city, window_start, window_end, n_readings, avg_temp_c, min_temp_c,
           max_temp_c, avg_humidity_pct, max_wind_kmh, total_precip_mm)
        VALUES %s
        ON CONFLICT (city, window_start) DO UPDATE SET
          window_end=EXCLUDED.window_end, n_readings=EXCLUDED.n_readings,
          avg_temp_c=EXCLUDED.avg_temp_c, min_temp_c=EXCLUDED.min_temp_c,
          max_temp_c=EXCLUDED.max_temp_c, avg_humidity_pct=EXCLUDED.avg_humidity_pct,
          max_wind_kmh=EXCLUDED.max_wind_kmh, total_precip_mm=EXCLUDED.total_precip_mm""",
        [(a.city, a.window_start, a.window_end, a.n_readings, a.avg_temp_c, a.min_temp_c,
          a.max_temp_c, a.avg_humidity_pct, a.max_wind_kmh, a.total_precip_mm) for a in rows])


def save_alerts(cur, rows: list[WeatherAlert]):
    execute_values(cur, """
        INSERT INTO staging.weather_alerts
          (city, observed_at, alert_type, metric_value, threshold, message)
        VALUES %s ON CONFLICT (city, observed_at, alert_type) DO NOTHING""",
        [(a.city, a.observed_at, a.alert_type, a.metric_value, a.threshold, a.message)
         for a in rows])


# ---------------------------------------------------------------- main loop
def main():
    consumer = make_consumer(TOPIC_RAW, "weather-processor")
    producer = make_producer()
    agg = WindowAggregator()
    conn = get_conn()
    last_msg = time.time()
    log.info("consuming %s ...", TOPIC_RAW)

    while True:
        batches = consumer.poll(timeout_ms=1000, max_records=500)
        readings = []
        for msgs in batches.values():
            for m in msgs:
                try:
                    readings.append(WeatherReading.model_validate(m.value))
                except Exception:
                    log.exception("bad message at offset %s; skipped", m.offset)

        aggs, alerts = [], []
        if readings:
            last_msg = time.time()
            for r in readings:
                agg.add(r)
                alerts.extend(check_alerts(r))
            aggs = agg.flush()
        elif time.time() - last_msg > IDLE_FLUSH_SECONDS:
            aggs = agg.flush(force=True)

        if not (readings or aggs):
            continue
        try:
            with conn, conn.cursor() as cur:
                if readings:
                    save_raw(cur, readings)
                if aggs:
                    save_aggs(cur, aggs)
                if alerts:
                    save_alerts(cur, alerts)
            for a in aggs:
                producer.send(TOPIC_AGG, key=a.city, value=a.model_dump(mode="json"))
            for al in alerts:
                producer.send(TOPIC_ALERTS, key=al.city, value=al.model_dump(mode="json"))
            producer.flush()
            consumer.commit()
            log.info("raw=%d aggregates=%d alerts=%d", len(readings), len(aggs), len(alerts))
        except Exception:
            log.exception("write failed; offsets not committed")
            conn.rollback()
            time.sleep(5)


if __name__ == "__main__":
    main()
