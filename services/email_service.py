"""Subscribes to order_placed and sends an order confirmation email (simulated with a log line)."""
from common import ORDER_PLACED, run_service


def handle(event: dict) -> None:
    print(f"[email] order {event['order_id']} ({event['items']} items, ${event['amount']:.2f}) "
          f"-> emailing confirmation to user {event['user_id']}", flush=True)


if __name__ == "__main__":
    run_service("email-service", ORDER_PLACED, handle)
