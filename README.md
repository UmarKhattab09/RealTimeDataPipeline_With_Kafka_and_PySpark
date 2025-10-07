# Real-Time Data Ingestion and Transformation Pipeline

## Project Overview
This project demonstrates a complete real-time data pipeline using **Apache Kafka**, **PySpark Streaming**, and **Apache Airflow**. It allows ingestion of streaming data, processing it in near real-time, and saving transformed results to CSV for further analysis.

The pipeline is structured to simulate a real-world ETL scenario:  
1. **Producer** generates streaming data (simulating live data from external APIs).  
2. **PySpark Streaming** consumes the data, applies transformations, and writes the results to CSV files.  
3. **Airflow DAGs** are configured to orchestrate tasks such as running producers and transformation scripts.

---

## Architecture
Producer (Kafka) --> Kafka Topics --> PySpark Streaming --> Transformations --> CSV Output
|
v
Airflow DAGs (Orchestration)


---

## Technologies Used
- **Apache Kafka** – Messaging system for real-time data ingestion  
- **PySpark Streaming** – Real-time data processing and transformation  
- **Apache Airflow** – Orchestration and workflow automation  
- **Docker & Docker Compose** – Containerized environment for reproducibility  
- **Python** – Scripting for producers, transformations, and orchestration  
- **CSV** – Storage of processed output  

---

## Key Features
- Real-time data ingestion and processing  
- Streaming transformations using PySpark  
- Fault-tolerant setup using Spark checkpoints  
- Scalable architecture supporting multiple Kafka brokers  
- Attempted workflow orchestration using Airflow (automation in progress)  

---

## How to Run
1. Start Kafka and Zookeeper using Docker Compose.  
2. Run the Python producer to generate data.  
3. Start the PySpark streaming job to consume and transform data.  
4. Optionally, start Airflow to orchestrate DAGs (paths must be configured inside container).

---

## Notes
- Currently, the Airflow DAG is set up but automation is limited due to container environment path issues.  
- The pipeline is fully functional locally for development and testing purposes.


## How To Run
- `docker compose -f "docker-compose-kafka.yaml" up -d
- python producer.py
- python spark/spark_stream.py
- python consumer.py (optional)
- python spark/transform.py

