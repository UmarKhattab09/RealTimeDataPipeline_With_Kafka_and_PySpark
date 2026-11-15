"""Shared Kafka helpers for the producer and the subscriber services."""
import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

# One topic per kind of event. Keep in sync with airflow/scripts/config.py.
CLICK_EVENTS = "click_events"
USER_SIGNED_IN = "user_signed_in"
ORDER_PLACED = "order_placed"
PAYMENT_COMPLETED = "payment_completed"


def _connect(factory, attempts: int = 30, delay: float = 2.0):
    """Kafka may still be starting when a container comes up, so retry for a while."""
    for attempt in range(1, attempts + 1):
        try:
            return factory()
        except KafkaError:
            if attempt == attempts:
                raise
            print(f"Kafka not ready, retrying ({attempt}/{attempts})...", flush=True)
            time.sleep(delay)


def make_producer() -> KafkaProducer:
    return _connect(lambda: KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    ))


def run_service(name: str, topic: str, handle) -> None:
    """Subscribe to one topic and call handle(event) for every message, forever.

    The group id makes Kafka remember how far this service has read, so after a
    restart it continues from where it stopped instead of re-processing events.
    """
    consumer = _connect(lambda: KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=name,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    ))
    print(f"[{name}] listening on topic '{topic}'", flush=True)
    try:
        for message in consumer:
            try:
                handle(message.value)
            except Exception as exc:  # one bad event must not stop the service
                print(f"[{name}] failed on offset {message.offset}: {exc!r}", flush=True)
    except KeyboardInterrupt:
        print(f"[{name}] stopping", flush=True)
    finally:
        consumer.close()
