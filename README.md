# Real-Time Data Ingestion and Transformation Pipeline

## Project Overview
A small real-time clickstream pipeline built with **Apache Kafka**, **PySpark Structured Streaming** and **Apache Airflow**.

1. **Producer** sends fake website click events (JSON) to the Kafka topic `click_events`.
2. **Spark Structured Streaming** reads the topic and appends the events to CSV files in `data/raw/`.
3. **Transform job** cleans the raw events and builds aggregates in `data/transformed/`.
4. **Airflow** runs the three steps in order every 5 minutes.

## Architecture
```
producer.py --> Kafka (click_events) --> spark_stream.py --> data/raw/*.csv --> transform.py --> data/transformed/
                                                                                                  ├── clean_events/
        Airflow DAG `spark_etl_dag`:                                                              ├── page_views_per_min/
        run_producer >> run_spark_stream >> run_transform                                         └── user_activity/
```

## Repository Layout
| Path | What it is |
|---|---|
| `docker-compose-kafka.yaml` | Zookeeper, 2 Kafka brokers, Kafdrop, Postgres and Airflow |
| `Dockerfile` | Airflow image with Java, PySpark and the Spark-Kafka connector |
| `airflow/dags/dags.py` | The `spark_etl_dag` DAG |
| `airflow/scripts/config.py` | Shared settings (Kafka address, topic, data folders), read from environment variables |
| `airflow/scripts/producer.py` | Kafka producer for fake click events |
| `airflow/scripts/spark_stream.py` | Kafka -> raw CSV streaming job |
| `airflow/scripts/transform.py` | Raw CSV -> cleaned events and aggregates |
| `data/` | Pipeline output (created at run time, not committed) |

## What the Transform Produces
- **`clean_events/`** – typed and de-duplicated events, with `event_date` and `ingest_lag_seconds` (time between Kafka receiving the event and Spark processing it).
- **`page_views_per_min/`** – views and unique users per page for each 1-minute window.
- **`user_activity/`** – per user: total events, distinct pages, first/last seen, and whether they reached `/checkout`.

The transform is a batch job that rebuilds its outputs from everything in `data/raw/`, so it is safe to run again and again.

## Run Everything with Docker (Airflow orchestrates)
```bash
docker compose -f docker-compose-kafka.yaml up -d --build
```
- Airflow UI: http://localhost:8080 (user `admin`, password `admin`). Switch on `spark_etl_dag`; it then runs every 5 minutes.
- Kafdrop (browse topics and messages): http://localhost:9000
- Output appears in `./data/` on your machine.

On Linux, create a `.env` file first so the container can write to `./data` and `./airflow/logs`:
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

## Run the Scripts Locally (without Airflow)
Requirements: Python, Java 11 or 17, and `pip install -r airflow/requirements.txt`.

```bash
docker compose -f docker-compose-kafka.yaml up -d zookeeper kafka1 kafka2 kafdrop
cd airflow/scripts
python producer.py            # sends an event every 2 seconds until Ctrl+C
python spark_stream.py        # in a second terminal: Kafka -> data/raw, runs until Ctrl+C
python transform.py           # builds data/transformed from data/raw
```

Useful options:
- `python producer.py --num-events 30 --interval 0.5` – send a fixed number of events and exit.
- `python spark_stream.py --once` – process everything new in Kafka and exit.

## Configuration
All settings have defaults for local runs and can be overridden with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address (`kafka1:29092,kafka2:29093` inside Docker) |
| `KAFKA_TOPIC` | `click_events` | Topic to produce to and read from |
| `PIPELINE_DATA_DIR` | `<repo>/data` | Where raw data, transformed data and checkpoints are written |
| `SPARK_KAFKA_PACKAGE` | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` | Spark-Kafka connector; must match the PySpark version |

## Technologies Used
- **Apache Kafka** – real-time message ingestion
- **PySpark Structured Streaming** – stream processing with checkpointed Kafka offsets
- **Apache Airflow** – orchestration
- **Docker & Docker Compose** – reproducible environment
