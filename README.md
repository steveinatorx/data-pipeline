# Data Pipeline

A local event streaming pipeline using Redpanda (Kafka-compatible) and an event simulator for generating and consuming event data.

## Overview

This project provides:
- **Redpanda**: Kafka-compatible streaming platform running in Docker
- **Event Simulator**: Python-based event generator that produces structured events
- **Consumers**: Kafka consumers for landing events to raw storage (NDJSON)
- **Jobs**: ETL jobs for converting raw data to optimized formats (Parquet)
- **DDL**: Schema definitions for storing events in data warehouses (Iceberg, Snowflake)

## Prerequisites

- Docker and Docker Compose
- Python 3.11.9 (managed via `.tool-versions` and `asdf` or similar)
- `pipenv` for Python dependency management
- `make` for running commands

## Quick Start

### 1. Start Redpanda

```bash
make up
```

This starts the Redpanda container and exposes:
- Kafka broker: `localhost:9092`
- Admin/metrics: `localhost:9644`

### 2. Create the Events Topic

```bash
make topic
```

Creates the `events` topic with 3 partitions and replication factor of 1.

### 3. Set Up Python Environment

```bash
pipenv install
```

This creates a virtual environment and installs dependencies (currently `kafka-python==2.0.2`).

### 4. Generate and Produce Events

```bash
# Generate events to local files (NDJSON)
make gen

# Produce events directly to Kafka (small batch: 200 orders)
make produce-small

# Produce events directly to Kafka (default: 5000 orders)
make produce

# Produce events directly to Kafka (large batch: 50000 orders)
make produce-big
```

### 5. Consume Events

```bash
# Consume all events from the beginning (via rpk)
make consume

# Consume only new events (via rpk)
make consume-latest

# Run Kafka consumer to sink events to raw NDJSON files
make sink-raw
```

### 6. Process Raw Data to Parquet

```bash
# Convert raw NDJSON to Parquet for a specific date
make raw-to-parquet DATE=2026-02-05

# Convert all available dates
make raw-to-parquet-all
```

### 7. Run Tests

```bash
make test
```

## Available Commands

### Docker Management

- `make up` - Start Redpanda container
- `make down` - Stop and remove Redpanda container
- `make restart` - Restart Redpanda container
- `make logs` - View Redpanda logs
- `make ps` - Show container status
- `make health` - Check if Redpanda is ready

### Kafka Topic Management

- `make topic` - Create the `events` topic
- `make topics` - List all topics
- `make describe-topic` - Describe the `events` topic configuration

### Event Consumption

- `make consume` - Consume all events from the beginning (via rpk)
- `make consume-latest` - Consume only new events (via rpk)
- `make sink-raw` - Run Kafka consumer to write events to raw NDJSON files

### Event Generation

- `make gen` - Generate events to local NDJSON files (no Kafka)
- `make produce` - Generate and produce events to Kafka (5000 orders)
- `make produce-small` - Generate and produce events to Kafka (200 orders)
- `make produce-big` - Generate and produce events to Kafka (50000 orders)

### Data Processing Jobs

- `make raw-to-parquet DATE=YYYY-MM-DD` - Convert raw NDJSON to Parquet for a specific date
- `make raw-to-parquet-all` - Convert all available dates from raw to Parquet

### Testing

- `make test` - Run all tests with pytest

### Python Environment

- `make venv` - Create Python virtual environment
- `make reqs` - Install Python requirements (requires `sim/requirements.txt`)

## Configuration

Key configuration variables in `makefile`:

- `KAFKA_BOOTSTRAP`: Kafka broker address (default: `localhost:9092`)
- `TOPIC`: Topic name (default: `events`)
- `PARTITIONS`: Number of partitions (default: `3`)
- `REPLICATION`: Replication factor (default: `1`)
- `N_ORDERS`: Default number of orders to generate (default: `5000`)
- `KAFKA_KEY_FIELD`: Field used as Kafka message key (default: `order_id`)
- `RAW_DIR`: Directory for raw NDJSON files (default: `./data/raw`)
- `PARQUET_DIR`: Directory for Parquet files (default: `./data/parquet`)
- `CONSUMER_GROUP`: Kafka consumer group name (default: `raw-sink`)
- `ROLL_MAX_MB`: Max file size before rolling (default: `64`)
- `ROLL_MAX_SECONDS`: Max file age before rolling (default: `60`)
- `ROWS_PER_FILE`: Max rows per Parquet file (default: `250000`)

Override any variable when running make:

```bash
make produce TOPIC=my-events N_ORDERS=10000
```

## Project Structure

```
.
├── consumers/
│   └── kafka_to_raw.py       # Kafka consumer: writes events to raw NDJSON files
├── jobs/
│   └── raw_to_parquet.py    # ETL job: converts raw NDJSON to Parquet
├── tests/
│   ├── test_kafka_to_raw.py  # Tests for Kafka consumer
│   └── test_raw_to_parquet.py # Tests for Parquet conversion job
├── docker/
│   ├── compose.yml           # Docker Compose configuration for Redpanda
│   └── makefile              # Alternative makefile location
├── ddl/
│   └── events.sql            # DDL for events table (Iceberg & Snowflake)
├── data/
│   ├── raw/                  # Raw NDJSON files (partitioned by ingest_date)
│   └── parquet/              # Parquet files (partitioned by ingest_date)
├── makefile                  # Main makefile with all commands
├── Pipfile                   # Pipenv dependencies
├── Pipfile.lock              # Locked dependencies
└── .tool-versions            # Python version specification
```

## Event Schema

Events follow a structured envelope pattern with:

**Envelope Fields:**
- `event_id` - Unique event identifier
- `event_type` - Type of event
- `schema_version` - Schema version for evolution
- `event_time` - When the event occurred
- `ingest_time` - When the event was ingested
- `tenant_id`, `user_id`, `session_id` - Context identifiers
- `source_system`, `environment`, `record_source` - Source metadata
- `payload` - JSON payload with event-specific data
- `checksum` - Data integrity check

See `ddl/events.sql` for complete table definitions for:
- **Iceberg/Lakehouse** (Spark, Trino, Athena)
- **Snowflake** (native tables)

## Data Pipeline Flow

1. **Event Generation**: Events are generated and produced to Kafka topic `events`
2. **Raw Landing**: `kafka_to_raw.py` consumer reads from Kafka and writes NDJSON files partitioned by `ingest_date`
3. **ETL Processing**: `raw_to_parquet.py` job converts NDJSON to Parquet with deduplication
4. **Data Warehouse**: Parquet files can be loaded into Iceberg tables or Snowflake

### Kafka Partitioning Strategy

Events are keyed by `order_id` to ensure:
- All events for a single order stay in the same partition
- Per-order ordering is preserved
- Parallel processing across partitions

### File Partitioning

Both raw NDJSON and Parquet files are partitioned by `ingest_date`:
```
data/
├── raw/events/ingest_date=2026-02-05/part-00000.ndjson
└── parquet/events/ingest_date=2026-02-05/part-00000.parquet
```

This partitioning enables:
- Efficient date-based queries and filtering
- Easy data retention policies
- Parallel processing by date range

## Development

### Using Pipenv

```bash
# Activate the virtual environment
pipenv shell

# Install a new package
pipenv install package-name

# Install a development dependency
pipenv install --dev package-name

# Run a command in the virtual environment
pipenv run python script.py
```

### Running Tests

```bash
# Run all tests
make test

# Or directly with pytest
pipenv run pytest tests/ -v

# Run specific test file
pipenv run pytest tests/test_kafka_to_raw.py -v

# Run with coverage
pipenv run pytest tests/ --cov=consumers --cov=jobs
```

### Writing Tests

Tests are located in the `tests/` directory and use `pytest`. Test files follow the pattern `test_*.py` and should mirror the structure of the code being tested.

Key testing principles:
- Use temporary directories for file I/O tests
- Mock external dependencies (Kafka, etc.) when possible
- Test edge cases and error conditions
- Keep tests fast and isolated

### Python Version

The project uses Python 3.11.9 as specified in `.tool-versions`. If using `asdf`:

```bash
asdf install python 3.11.9
asdf local python 3.11.9
```

## Troubleshooting

### Redpanda not starting
- Check Docker is running: `docker ps`
- Check ports 9092 and 9644 are available
- View logs: `make logs`

### Topic creation fails
- Ensure Redpanda is running: `make health`
- Check if topic already exists: `make topics`

### Python dependencies issues
- Ensure Python 3.11.9 is installed
- Reinstall dependencies: `pipenv install --dev`
- Check `sim/requirements.txt` exists if using `make reqs`

### Tests failing
- Ensure all dependencies are installed: `pipenv install --dev`
- Check that `pyarrow` and `pytest` are available: `pipenv run pytest --version`
- Run tests with verbose output: `pipenv run pytest tests/ -v`

### Consumer not processing messages
- Verify Kafka topic exists: `make topics`
- Check consumer group offsets: `docker exec -it redpanda rpk group describe raw-sink --brokers redpanda:29092`
- Ensure events are being produced: `make consume-latest`

## License

[Add your license here]
