import json
import os
from functools import lru_cache

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


@lru_cache
def get_producer() -> KafkaProducer:
    """Lazily create one producer per process and reuse it — creating a new
    KafkaProducer per request would be slow and would leak connections."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        max_block_ms=5000,      # fail fast if the broker is down instead of hanging the request
        request_timeout_ms=5000,
    )


def publish(topic: str, key: str, value: dict) -> None:
    producer = get_producer()
    producer.send(topic, key=key, value=value)
    producer.flush(timeout=5)