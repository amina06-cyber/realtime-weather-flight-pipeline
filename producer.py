"""Open-Meteo producer.

  python producer.py live --interval 900 --batch-size 5
  python producer.py backfill --days 30

live     polls the Forecast API ("current" block) for all cities, in batches.
backfill pulls hourly history from the Archive API, one city at a time.
Both publish WeatherReading messages to `weather-raw`, keyed by city.
"""
import argparse
import time
from datetime import date, datetime, timedelta

import requests

from common import (CITIES, TOPIC_RAW, TZ, WeatherReading, ensure_topics,
                    get_logger, make_producer)

log = get_logger("producer")

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,pressure_msl"


def get_json(url: str, params: dict, retries: int = 4) -> dict | list:
    """GET with exponential backoff (handles 429 / transient errors)."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                raise requests.HTTPError("rate limited (429)")
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 2 ** attempt * 5
            log.warning("request failed (%s); retry in %ss", e, wait)
            time.sleep(wait)
    raise RuntimeError(f"giving up on {url}")


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=TZ)  # API returns local naive time


def fetch_live(batch: list[str]) -> list[WeatherReading]:
    """One request for several cities (Open-Meteo accepts comma-separated coords)."""
    params = {
        "latitude": ",".join(str(CITIES[c][0]) for c in batch),
        "longitude": ",".join(str(CITIES[c][1]) for c in batch),
        "current": VARS,
        "timezone": "Asia/Karachi",
    }
    data = get_json(FORECAST_URL, params)
    data = data if isinstance(data, list) else [data]  # single city -> dict
    out = []
    for city, d in zip(batch, data):
        c = d["current"]
        out.append(WeatherReading(
            city=city, observed_at=parse_ts(c["time"]),
            temperature_c=c.get("temperature_2m"),
            humidity_pct=c.get("relative_humidity_2m"),
            wind_speed_kmh=c.get("wind_speed_10m"),
            precipitation_mm=c.get("precipitation"),
            pressure_hpa=c.get("pressure_msl"),
            source="live"))
    return out


def fetch_backfill(city: str, start: date, end: date) -> list[WeatherReading]:
    lat, lon = CITIES[city]
    d = get_json(ARCHIVE_URL, {
        "latitude": lat, "longitude": lon,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "hourly": VARS, "timezone": "Asia/Karachi"})
    h = d["hourly"]
    return [WeatherReading(
        city=city, observed_at=parse_ts(t),
        temperature_c=h["temperature_2m"][i],
        humidity_pct=h["relative_humidity_2m"][i],
        wind_speed_kmh=h["wind_speed_10m"][i],
        precipitation_mm=h["precipitation"][i],
        pressure_hpa=h["pressure_msl"][i],
        source="backfill") for i, t in enumerate(h["time"])]


def publish(producer, readings: list[WeatherReading]) -> None:
    for r in readings:
        producer.send(TOPIC_RAW, key=r.city, value=r.model_dump(mode="json"))
    producer.flush()
    log.info("published %d readings", len(readings))


def run_live(interval: int, batch_size: int) -> None:
    producer = make_producer()
    cities = list(CITIES)
    while True:
        for i in range(0, len(cities), batch_size):
            try:
                publish(producer, fetch_live(cities[i:i + batch_size]))
            except Exception:
                log.exception("live batch failed; continuing")
            time.sleep(2)  # small pause between batches (fair-use)
        log.info("cycle done; sleeping %ss", interval)
        time.sleep(interval)


def run_backfill(days: int, end_lag: int) -> None:
    producer = make_producer()
    end = date.today() - timedelta(days=end_lag)  # archive lags a couple of days
    start = end - timedelta(days=days - 1)
    for city in CITIES:
        log.info("backfill %s %s -> %s", city, start, end)
        publish(producer, fetch_backfill(city, start, end))
        time.sleep(3)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    lv = sub.add_parser("live")
    lv.add_argument("--interval", type=int, default=900, help="seconds between cycles")
    lv.add_argument("--batch-size", type=int, default=5, help="cities per API request")
    bf = sub.add_parser("backfill")
    bf.add_argument("--days", type=int, default=30)
    bf.add_argument("--end-lag", type=int, default=2, help="days before today the archive is complete")
    args = ap.parse_args()

    ensure_topics()
    if args.mode == "live":
        run_live(args.interval, args.batch_size)
    else:
        run_backfill(args.days, args.end_lag)
