# LTA Bus Arrivals — Real-Time Streaming Pipeline

A real-time data pipeline that ingests Singapore bus arrival data from the [LTA DataMall API](https://datamall.lta.gov.sg/content/datamall/en.html), streams it through **Apache Kafka**, processes it with **Apache Flink** (joining to a static bus stop mapping), and writes enriched results to **Parquet** files.

## Architecture

```
LTA DataMall API ──► bus_producer.py ──► Kafka ──► Flink (flink_processor.py) ──► Parquet Files
                                                       │
                                               bus_stop_mapping.parquet
                                              (static lookup table)
```

| Component | Role |
|-----------|------|
| `bus_producer.py` | Polls the LTA API every 60s for bus arrivals and publishes raw JSON to Kafka |
| `fetch_bus_stops.py` | One-time script to download all bus stop metadata and save as a Parquet mapping file |
| `flink_processor.py` | PyFlink streaming job that reads from Kafka, joins to the bus stop mapping, and writes enriched Parquet output |

## Prerequisites

- **Docker & Docker Compose** (recommended), or
- **Python 3.11+**, **Apache Kafka**, **PyFlink 2.2.0**, **JDK 11+** (for local development)
- An **LTA DataMall API key** — [register here](https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html)

## Quick Start (Docker)

### 1. Configure Environment

```bash
cp .env.example .env
# Edit .env and add your real API key
```

### 2. Generate Bus Stop Mapping

This creates the static Parquet file that Flink joins against. Run this **once** before starting the pipeline:

```bash
pip install requests pandas pyarrow python-dotenv
python fetch_bus_stops.py
```

This creates `data/mappings/bus_stop_mapping.parquet`.

### 3. Start the Pipeline

```bash
docker compose up --build
```

This starts:
- **Kafka** (KRaft mode, no Zookeeper)
- **Flink JobManager + TaskManager**
- **Bus Producer** (polls LTA API → Kafka)
- **Flink Job Submitter** (submits the PyFlink job)

### 4. Check Output

Enriched results are written to `data/output/` as Parquet files. Files are committed on each Flink checkpoint (every 30 seconds).

The Flink Web UI is available at [http://localhost:8081](http://localhost:8081).

## Local Development (Without Docker)

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Download Connector JARs

The Flink processor requires connector JARs that match PyFlink 2.2.0. Download them into `flink_setup/connectors/`:

```bash
mkdir -p flink_setup/connectors
cd flink_setup/connectors

# Kafka connector
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

# Parquet format + dependencies
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/2.2.0/flink-parquet-2.2.0.jar
curl -O https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.15.2/parquet-hadoop-1.15.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.15.2/parquet-column-1.15.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.15.2/parquet-common-1.15.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.15.2/parquet-encoding-1.15.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/parquet/parquet-format-structures/1.15.2/parquet-format-structures-1.15.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/parquet/parquet-jackson/1.15.2/parquet-jackson-1.15.2.jar

# Hadoop dependencies
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.6/hadoop-mapreduce-client-core-3.3.6.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar
curl -O https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar
curl -O https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar
```

### 3. Run the Pipeline

> **Note:** The `.ps1` helper scripts assume Kafka and Flink binaries are installed under `flink_setup/` and `k/` within the project directory.

```bash
# Start Kafka (PowerShell)
.\start_kafka.ps1

# Start Flink cluster (PowerShell)
.\start_flink.ps1

# Run the producer
python bus_producer.py

# Submit the Flink job (in a separate terminal)
python flink_processor.py

# View Kafka messages (optional)
.\view_messages.ps1
```

## Project Structure

```
├── bus_producer.py         # Kafka producer — LTA API → Kafka
├── fetch_bus_stops.py      # One-time bus stop mapping generator
├── flink_processor.py      # PyFlink streaming job
├── docker-compose.yml      # Full pipeline orchestration
├── Dockerfile.flink        # Flink image with PyFlink + JARs
├── Dockerfile.producer     # Lightweight producer image
├── .env.example            # Template for API key
├── requirements.txt        # Python dependencies
├── start_kafka.ps1         # Local Kafka startup (PowerShell)
├── start_flink.ps1         # Local Flink startup (PowerShell)
├── stop_flink.ps1          # Local Flink shutdown (PowerShell)
├── view_messages.ps1       # Peek at Kafka messages (PowerShell)
└── data/
    ├── mappings/            # Bus stop mapping Parquet file
    └── output/              # Flink output (Parquet files)
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LTA_ACCOUNT_KEY` | — | LTA DataMall API key (required) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `FLINK_OUTPUT_PATH` | `./data/output` | Flink Parquet output directory |
| `FLINK_MAPPING_PATH` | `./data/mappings` | Bus stop mapping Parquet directory |
| `DOCKER_ENV` | — | Set to `true` inside Docker containers |
