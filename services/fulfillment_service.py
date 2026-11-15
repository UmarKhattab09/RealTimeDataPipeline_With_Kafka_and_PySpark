"""Subscribes to payment_completed: ships paid orders, asks the user to retry failed payments.

Both actions are simulated with a log line.
"""
from common import PAYMENT_COMPLETED, run_service


def handle(event: dict) -> None:
    if event["status"] == "success":
        print(f"[fulfillment] payment for {event['order_id']} received "
              f"-> shipping order to user {event['user_id']}", flush=True)
    else:
        print(f"[fulfillment] payment for {event['order_id']} FAILED "
              f"-> asking user {event['user_id']} to retry", flush=True)


if __name__ == "__main__":
    run_service("fulfillment-service", PAYMENT_COMPLETED, handle)
