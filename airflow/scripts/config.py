"""Shared settings for the pipeline scripts.

Everything is read from environment variables so the same scripts run both on
a laptop (defaults below) and inside the Airflow container (values set in
docker-compose-kafka.yaml).
"""
import os
from pathlib import Path

# Repo root when run locally; overridden with PIPELINE_DATA_DIR in the container.
_DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "click_events")

DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", _DEFAULT_DATA_DIR)).resolve()
RAW_DIR = DATA_DIR / "raw"                  # Kafka -> CSV (spark_stream.py)
TRANSFORMED_DIR = DATA_DIR / "transformed"  # cleaned + aggregated (transform.py)
CHECKPOINT_DIR = DATA_DIR / "checkpoints"

# Must match the installed pyspark version.
SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def uri(path: Path) -> str:
    """file:// URI that Spark accepts on Linux, macOS and Windows."""
    return Path(path).as_uri()
