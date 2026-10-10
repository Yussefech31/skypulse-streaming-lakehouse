.PHONY: help setup lint format test run-ingestion run-stream run-dashboard benchmark compose-up compose-down clean

PYTHON ?= python

help:
	@echo "Available commands:"
	@echo "  make setup         : Install dependencies"
	@echo "  make lint          : Check code style with ruff"
	@echo "  make format        : Auto-format code with ruff"
	@echo "  make test          : Run unit tests with pytest"
	@echo "  make run-ingestion : Start ADS-B ingestion worker"
	@echo "  make run-stream    : Run streaming processor"
	@echo "  make run-dashboard : Launch Streamlit live map"
	@echo "  make benchmark     : Execute throughput & latency benchmarks"
	@echo "  make compose-up    : Start Kafka & MinIO via Docker Compose"
	@echo "  make compose-down  : Stop Docker Compose services"

setup:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

test:
	$(PYTHON) -m pytest -v

run-ingestion:
	$(PYTHON) -m src.ingestion.kafka_producer

run-stream:
	$(PYTHON) -m src.streaming.spark_stream

run-dashboard:
	streamlit run dashboard/app.py

benchmark:
	$(PYTHON) scripts/benchmark.py

compose-up:
	docker compose -f docker/docker-compose.yml up -d

compose-down:
	docker compose -f docker/docker-compose.yml down

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__ lakehouse checkpoints results/*.png
