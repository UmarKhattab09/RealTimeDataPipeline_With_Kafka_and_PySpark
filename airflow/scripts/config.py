"""Shared settings for the Spark jobs.

Everything is read from environment variables so the same scripts run both on
a laptop (defaults below) and inside the Airflow container (values set in
docker-compose-kafka.yaml).
"""
import os
from pathlib import Path

# Repo root when run locally; overridden with PIPELINE_DATA_DIR in the container.
_DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Keep in sync with services/common.py.
TOPICS = ["click_events", "user_signed_in", "order_placed", "payment_completed"]

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", _DEFAULT_DATA_DIR)).resolve()
CHECKPOINT_DIR = DATA_DIR / "checkpoints"

# The analytics database ("warehouse"). Demo credentials, same as in the compose file.
WAREHOUSE_JDBC_URL = os.getenv("WAREHOUSE_JDBC_URL", "jdbc:postgresql://localhost:5433/warehouse")
JDBC_PROPERTIES = {
    "user": os.getenv("WAREHOUSE_USER", "warehouse"),
    "password": os.getenv("WAREHOUSE_PASSWORD", "warehouse"),
    "driver": "org.postgresql.Driver",
}

# Kafka connector (must match the installed pyspark version) and Postgres driver.
SPARK_PACKAGES = os.getenv(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
)


def uri(path: Path) -> str:
    """file:// URI that Spark accepts on Linux, macOS and Windows."""
    return Path(path).as_uri()
