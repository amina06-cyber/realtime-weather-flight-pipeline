"""Shared config, message schemas and Kafka/Postgres helpers for the weather pipeline."""
import json
import logging
import os
from datetime import datetime
from typing import Literal, Optional
from zoneinfo import ZoneInfo

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pydantic import BaseModel

# ---------------------------------------------------------------- config
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "weather-raw"
TOPIC_AGG = "weather-aggregates"
TOPIC_ALERTS = "weather-alerts"
PARTITIONS = 3

TZ = ZoneInfo("Asia/Karachi")
WINDOW_SECONDS = 300  # 5-minute tumbling window

# city -> (latitude, longitude)
CITIES = {
    "Lahore": (31.5497, 74.3436),
    "Karachi": (24.8607, 67.0011),
    "Islamabad": (33.6844, 73.0479),
    "Peshawar": (34.0151, 71.5249),
    "Quetta": (30.1798, 66.9750),
}

# alert thresholds: metric column -> (comparison, limit, alert_type)
THRESHOLDS = [
    ("temperature_c", ">", 40.0, "HEAT"),
    ("temperature_c", "<", 0.0, "FREEZE"),
    ("wind_speed_kmh", ">", 50.0, "HIGH_WIND"),
    ("precipitation_mm", ">", 10.0, "HEAVY_RAIN"),
]

DB_PARAMS = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "weather"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
)


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return logging.getLogger(name)


# ---------------------------------------------------------------- schemas
class WeatherReading(BaseModel):
    city: str
    observed_at: datetime  # timezone-aware
    temperature_c: Optional[float] = None
    humidity_pct: Optional[float] = None
    wind_speed_kmh: Optional[float] = None
    precipitation_mm: Optional[float] = None
    pressure_hpa: Optional[float] = None
    source: Literal["live", "backfill"]


class WeatherAggregate(BaseModel):
    city: str
    window_start: datetime
    window_end: datetime
    n_readings: int
    avg_temp_c: Optional[float]
    min_temp_c: Optional[float]
    max_temp_c: Optional[float]
    avg_humidity_pct: Optional[float]
    max_wind_kmh: Optional[float]
    total_precip_mm: Optional[float]


class WeatherAlert(BaseModel):
    city: str
    observed_at: datetime
    alert_type: str
    metric_value: float
    threshold: float
    message: str


# ---------------------------------------------------------------- kafka
def ensure_topics() -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    for t in (TOPIC_RAW, TOPIC_AGG, TOPIC_ALERTS):
        try:
            admin.create_topics([NewTopic(t, num_partitions=PARTITIONS, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    admin.close()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=5,
        linger_ms=50,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )


def make_consumer(topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group_id,
        enable_auto_commit=False,  # commit only after the DB write succeeds
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )


# ---------------------------------------------------------------- postgres
def get_conn():
    return psycopg2.connect(**DB_PARAMS)
