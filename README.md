# Real-Time Event Pipeline with Kafka, PySpark and Airflow

## Project Overview
A small event-driven system built with **Apache Kafka**, **PySpark Structured Streaming**, **Postgres** and **Apache Airflow**.

A simulated website publishes four kinds of events to Kafka. Two groups of subscribers read them:

- **Small services** react to single events as they happen (send an email, ship an order).
- **Spark** reads all events, stores them in Postgres and builds analytics tables. Airflow schedules these jobs.

## Architecture
```
                                        ┌─> notification_service   (user_signed_in    -> sign-in alert)
                                        ├─> email_service          (order_placed      -> confirmation email)
producer.py ──> Kafka topics ───────────┼─> fulfillment_service    (payment_completed -> ship / ask to retry)
(simulated      click_events            │
 website)       user_signed_in          └─> spark_stream.py ──> Postgres raw tables ──> transform.py ──> Postgres analytics tables
                order_placed                └──────────── Airflow DAG `spark_etl_dag`, every 5 minutes ───────────┘
                payment_completed
```

### What runs where
| Part | Runs as | Why |
|---|---|---|
| Producer and the three services | Always-on containers | They react to events continuously, so there is nothing to schedule |
| `spark_stream.py`, `transform.py` | Airflow tasks | They start, finish and repeat on a schedule |

## Events
| Topic | Fields (besides `event_id`, `user_id`, `timestamp`) | When |
|---|---|---|
| `click_events` | `page` | Every page view |
| `user_signed_in` | `device` | A user signs in |
| `order_placed` | `order_id`, `amount`, `items` | A user buys on `/checkout` |
| `payment_completed` | `order_id`, `amount`, `method`, `status` (`success` / `failed`) | The payment for an order finishes |

## Tables in Postgres (`warehouse` database)
**Raw tables** (written by `spark_stream.py`, one per topic): `click_events`, `user_signed_in`, `order_placed`, `payment_completed`.

**Analytics tables** (rebuilt by `transform.py` on every run):
- **`page_views_per_min`** – views and unique users per page for each 1-minute window.
- **`order_status`** – one row per order with its payment result, method and `seconds_to_pay`.
- **`revenue_per_min`** – paid orders, revenue and failed payments per 1-minute window.
- **`user_activity`** – per user: clicks, sign-ins, orders, total spent, first/last seen, reached checkout.

## Repository Layout
| Path | What it is |
|---|---|
| `docker-compose-kafka.yaml` | Kafka (2 brokers), Zookeeper, Kafdrop, producer, services, warehouse Postgres, Airflow and its Postgres |
| `services/producer.py` | Simulated website backend that publishes the events |
| `services/notification_service.py`, `email_service.py`, `fulfillment_service.py` | Subscribers, one per business topic |
| `services/common.py` | Topic names and Kafka connection helpers for the above |
| `services/Dockerfile` | Image for the producer and services |
| `airflow/scripts/spark_stream.py` | Kafka -> Postgres raw tables |
| `airflow/scripts/transform.py` | Raw tables -> analytics tables |
| `airflow/scripts/config.py` | Settings for the Spark jobs, read from environment variables |
| `airflow/dags/dags.py` | The `spark_etl_dag` DAG |
| `Dockerfile` | Airflow image with Java, PySpark, the Spark-Kafka connector and the Postgres driver |

## Run Everything with Docker
On Linux, first let the Airflow container write to `./data` and `./airflow/logs`:
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

Start the stack:
```bash
docker compose -f docker-compose-kafka.yaml up -d --build
```

Then:
- **Airflow UI:** http://localhost:8080 (user `admin`, password `admin`). Switch on `spark_etl_dag`; it then runs every 5 minutes.
- **Kafdrop** (browse topics and messages): http://localhost:9000
- **Watch the services react:**
  ```bash
  docker compose -f docker-compose-kafka.yaml logs -f producer notification-service email-service fulfillment-service
  ```
- **Query the results** (password `warehouse`):
  ```bash
  docker compose -f docker-compose-kafka.yaml exec warehouse psql -U warehouse warehouse -c "select * from revenue_per_min order by 1"
  ```
  From your own machine the database is at `localhost:5433`.

### Starting over
The warehouse data lives in a Docker volume and Spark's Kafka position lives in `./data/checkpoints`. Reset both together, otherwise Spark will skip events the database no longer has:
```bash
docker compose -f docker-compose-kafka.yaml down -v
```
```bash
rm -rf data
```

## Run the Scripts Locally (without Airflow)
Requirements: Python, Java 11 or 17, and `pip install -r airflow/requirements.txt`.

```bash
docker compose -f docker-compose-kafka.yaml up -d zookeeper kafka1 kafka2 kafdrop warehouse
```
Then, each in its own terminal:
```bash
cd services
python producer.py                # publishes events until Ctrl+C
python email_service.py           # same for notification_service.py and fulfillment_service.py
```
```bash
cd airflow/scripts
python spark_stream.py            # Kafka -> Postgres, one micro-batch every 10 seconds, until Ctrl+C
python transform.py               # rebuild the analytics tables
```

Useful options:
- `python producer.py --num-events 50 --interval 0.2` – send about 50 events and exit.
- `python spark_stream.py --once` – process everything new in Kafka and exit (what Airflow runs).

## Configuration
All settings have defaults for local runs and can be overridden with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address (`kafka1:29092,kafka2:29093` inside Docker) |
| `WAREHOUSE_JDBC_URL` | `jdbc:postgresql://localhost:5433/warehouse` | Analytics database |
| `WAREHOUSE_USER` / `WAREHOUSE_PASSWORD` | `warehouse` / `warehouse` | Demo credentials for that database |
| `PIPELINE_DATA_DIR` | `<repo>/data` | Where Spark keeps its checkpoints |
| `SPARK_PACKAGES` | Kafka connector 3.5.1 + Postgres driver 42.7.3 | Spark dependencies; the connector must match the PySpark version |

## Notes and Limits
- The services only log what they would do; no real email or shipment is sent.
- Each service has its own Kafka consumer group, so after a restart it continues where it stopped.
- The raw tables are append-only. If a Spark batch is retried, an event can be stored twice; `transform.py` removes such duplicates by `event_id`.
- Topic names are defined in two places (`services/common.py` and `airflow/scripts/config.py`); change both together.
