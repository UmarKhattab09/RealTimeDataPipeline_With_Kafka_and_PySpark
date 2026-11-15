"""Simulates the website backend: turns user activity into Kafka events.

Every step a random user does something:
  - sometimes signs in            -> user_signed_in
  - views a page                  -> click_events
  - on /checkout, may buy         -> order_placed, then payment_completed (success or failed)

    python producer.py                  # run until Ctrl+C
    python producer.py --num-events 50  # stop after about 50 events
"""
import argparse
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator

from common import (CLICK_EVENTS, ORDER_PLACED, PAYMENT_COMPLETED, USER_SIGNED_IN,
                    make_producer)

PAGES = ["/home", "/product/123", "/product/456", "/cart", "/checkout"]
DEVICES = ["web", "ios", "android"]
PAYMENT_METHODS = ["card", "paypal", "apple_pay"]


def base_event(user_id: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def simulate_step() -> Iterator[tuple[str, dict]]:
    """One user action, yielded as (topic, event) pairs as they happen."""
    user_id = random.randint(1, 100)

    if random.random() < 0.15:
        yield USER_SIGNED_IN, {**base_event(user_id), "device": random.choice(DEVICES)}

    page = random.choice(PAGES)
    yield CLICK_EVENTS, {**base_event(user_id), "page": page}

    if page == "/checkout" and random.random() < 0.6:
        order_id = f"ord-{uuid.uuid4().hex[:8]}"
        amount = round(random.uniform(10, 250), 2)
        yield ORDER_PLACED, {**base_event(user_id), "order_id": order_id,
                             "amount": amount, "items": random.randint(1, 5)}
        time.sleep(random.uniform(0.2, 1.5))  # the payment provider takes a moment
        yield PAYMENT_COMPLETED, {**base_event(user_id), "order_id": order_id,
                                  "amount": amount,
                                  "method": random.choice(PAYMENT_METHODS),
                                  "status": "success" if random.random() < 0.9 else "failed"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--num-events", type=int, default=0,
                        help="stop after about this many events; 0 means run forever")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds to wait between user actions")
    args = parser.parse_args()

    producer = make_producer()
    print("Producer started", flush=True)
    sent = 0
    try:
        while args.num_events == 0 or sent < args.num_events:
            for topic, event in simulate_step():
                # Raises if the broker does not acknowledge the message.
                producer.send(topic, event).get(timeout=30)
                sent += 1
                print(f"{topic}: {event}", flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
    print(f"Done, sent {sent} events")


if __name__ == "__main__":
    main()
