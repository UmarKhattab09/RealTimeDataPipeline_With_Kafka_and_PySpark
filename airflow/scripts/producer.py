"""Send fake click events to Kafka.

    python producer.py                  # run until Ctrl+C
    python producer.py --num-events 20  # send 20 events and exit (used by Airflow)
"""
import argparse
import json
import random
import time
import uuid

from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

PAGES = ["/home", "/cart", "/product/123", "/checkout"]


def make_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "page": random.choice(PAGES),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-events", type=int, default=0,
                        help="number of events to send; 0 means run forever")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="seconds to wait between events")
    args = parser.parse_args()

    print(f"Starting producer -> {KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    sent = 0
    try:
        while args.num_events == 0 or sent < args.num_events:
            event = make_event()
            # Raises if the broker does not acknowledge, so an Airflow task fails loudly.
            metadata = producer.send(KAFKA_TOPIC, event).get(timeout=30)
            sent += 1
            print(f"Sent {event} -> partition {metadata.partition} offset {metadata.offset}")
            if args.num_events == 0 or sent < args.num_events:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
    print(f"Done, sent {sent} events")


if __name__ == "__main__":
    main()
