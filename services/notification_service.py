"""Subscribes to user_signed_in and sends a sign-in alert (simulated with a log line)."""
from common import USER_SIGNED_IN, run_service


def handle(event: dict) -> None:
    print(f"[notification] user {event['user_id']} signed in on {event['device']} "
          f"-> sending 'new sign-in' alert", flush=True)


if __name__ == "__main__":
    run_service("notification-service", USER_SIGNED_IN, handle)
