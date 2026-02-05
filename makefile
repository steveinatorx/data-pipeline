SHELL := /bin/bash

# ---------- Config ----------
COMPOSE ?= docker compose
COMPOSE_FILE ?= docker/compose.yml

KAFKA_BOOTSTRAP ?= localhost:9092
TOPIC ?= events
PARTITIONS ?= 3
REPLICATION ?= 1

PY ?= python3
SIM ?= sim/event_sim.py
OUT ?= ./out
SEED ?= 42

# Smaller / bigger runs
N_ORDERS_SMALL ?= 200
N_ORDERS ?= 5000

# Kafka key strategy: order_id keeps per-order ordering within a partition
KAFKA_KEY_FIELD ?= order_id

# ---------- Targets ----------
.PHONY: help up down restart logs ps health \
        topic topics describe-topic \
        consume consume-latest \
        venv reqs gen produce produce-small produce-big

help:
	@echo ""
	@echo "Local Redpanda + event simulator"
	@echo ""
	@echo "Docker:"
	@echo "  make up | down | restart | logs | ps | health"
	@echo ""
	@echo "Kafka:"
	@echo "  make topic | topics | describe-topic"
	@echo "  make consume | consume-latest"
	@echo ""
	@echo "Sim:"
	@echo "  make venv reqs"
	@echo "  make gen (writes NDJSON to OUT)"
	@echo "  make produce / produce-small / produce-big"
	@echo ""

# ----- Docker -----
up:
	$(COMPOSE) -f $(COMPOSE_FILE) up -d
	@echo "Redpanda up. Bootstrap: $(KAFKA_BOOTSTRAP)"

down:
	$(COMPOSE) -f $(COMPOSE_FILE) down -v

restart: down up

logs:
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f redpanda

ps:
	$(COMPOSE) -f $(COMPOSE_FILE) ps

health:
	@curl -fsS http://localhost:9644/v1/status/ready && echo "OK" || (echo "NOT READY"; exit 1)

# ----- Kafka via rpk (inside container) -----
topic:
	docker exec -it redpanda rpk topic create $(TOPIC) \
		--brokers redpanda:29092 \
		--partitions $(PARTITIONS) \
		--replicas $(REPLICATION) || true
	@$(MAKE) topics

topics:
	docker exec -it redpanda rpk topic list --brokers redpanda:29092

describe-topic:
	docker exec -it redpanda rpk topic describe $(TOPIC) --brokers redpanda:29092

consume:
	docker exec -it redpanda rpk topic consume $(TOPIC) --brokers redpanda:29092 -f json -o start

consume-latest:
	docker exec -it redpanda rpk topic consume $(TOPIC) --brokers redpanda:29092 -f json

# ----- Python -----
venv:
	@test -d .venv || $(PY) -m venv .venv
	@echo "Created .venv"

reqs: venv
	. .venv/bin/activate && pip install -U pip && pip install -r sim/requirements.txt

gen: reqs
	. .venv/bin/activate && \
	$(PY) $(SIM) --out $(OUT) --n-orders $(N_ORDERS) --seed $(SEED)

produce: reqs
	. .venv/bin/activate && \
	$(PY) $(SIM) --out $(OUT) --n-orders $(N_ORDERS) --seed $(SEED) \
	  --kafka-bootstrap $(KAFKA_BOOTSTRAP) --kafka-topic $(TOPIC) \
	  --kafka-key-field $(KAFKA_KEY_FIELD)

produce-small: reqs
	. .venv/bin/activate && \
	$(PY) $(SIM) --out $(OUT) --n-orders $(N_ORDERS_SMALL) --seed $(SEED) \
	  --kafka-bootstrap $(KAFKA_BOOTSTRAP) --kafka-topic $(TOPIC) \
	  --kafka-key-field $(KAFKA_KEY_FIELD)

produce-big: reqs
	. .venv/bin/activate && \
	$(PY) $(SIM) --out $(OUT) --n-orders 50000 --seed $(SEED) \
	  --kafka-bootstrap $(KAFKA_BOOTSTRAP) --kafka-topic $(TOPIC) \
	  --kafka-key-field $(KAFKA_KEY_FIELD)

