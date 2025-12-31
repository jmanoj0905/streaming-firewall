import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

TOPIC = "firewall-logs"
LEVELS = ["INFO", "WARN", "ERROR", "DEBUG", "DROP"]
SOURCES = ["producer", "unknown-service", "servers", None]
BOOTSTRAP_SERVERS = "localhost:9092"
IST = ZoneInfo("Asia/Kolkata")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)


def build_event():
    bad = random.choice([True, False])
    event = {
        "event_id": None if bad and random.random() < 0.4 else str(uuid.uuid4()),
        "source": random.choice(SOURCES),
        "level": random.choice(LEVELS),
        "message": "producer event",
        "timestamp": (
            "INVALID_TIME"
            if bad and random.random() < 0.4
            else datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
        )
    }
    return event

try:
    while True:
        event = build_event()
        producer.send(TOPIC, event)
        print("Sent : ", event)
        time.sleep(1)


except KeyboardInterrupt:
    print("Stopping producer!")
    producer.close()
