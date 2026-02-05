# Data Pipeline

A local event streaming pipeline using Redpanda (Kafka-compatible) and an event simulator for generating and consuming event data.

## Overview

This project provides:
- **Redpanda**: Kafka-compatible streaming platform running in Docker
- **Event Simulator**: Python-based event generator that produces structured events
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
# Consume all events from the beginning
make consume

# Consume only new events
make consume-latest
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

- `make consume` - Consume all events from the beginning
- `make consume-latest` - Consume only new events

### Event Generation

- `make gen` - Generate events to local NDJSON files (no Kafka)
- `make produce` - Generate and produce events to Kafka (5000 orders)
- `make produce-small` - Generate and produce events to Kafka (200 orders)
- `make produce-big` - Generate and produce events to Kafka (50000 orders)

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

Override any variable when running make:

```bash
make produce TOPIC=my-events N_ORDERS=10000
```

## Project Structure

```
.
├── docker/
│   ├── compose.yml          # Docker Compose configuration for Redpanda
│   └── makefile              # Alternative makefile location
├── ddl/
│   └── events.sql            # DDL for events table (Iceberg & Snowflake)
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

## Kafka Partitioning Strategy

Events are keyed by `order_id` to ensure:
- All events for a single order stay in the same partition
- Per-order ordering is preserved
- Parallel processing across partitions

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

## License

[Add your license here]
