.PHONY: help build up down restart logs ps clean generate-data test

help:
	@echo "Super Mario Maker Data Pipeline - Make Commands"
	@echo ""
	@echo "Available commands:"
	@echo "  make build          - Build Docker images"
	@echo "  make up             - Start all services"
	@echo "  make down           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make logs           - View logs (follow mode)"
	@echo "  make logs-scheduler - View scheduler logs"
	@echo "  make logs-webserver - View webserver logs"
	@echo "  make ps             - List running services"
	@echo "  make clean          - Remove containers and volumes"
	@echo "  make generate-data  - Generate sample data"
	@echo "  make test           - Validate DAG syntax"
	@echo "  make db-shell       - Connect to PostgreSQL"
	@echo "  make airflow-shell  - Connect to Airflow container"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"

down:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f

logs-scheduler:
	docker-compose logs -f airflow-scheduler

logs-webserver:
	docker-compose logs -f airflow-webserver

ps:
	docker-compose ps

clean:
	docker-compose down -v
	rm -rf logs/*
	@echo "Cleaned up containers, volumes, and logs"

generate-data:
	python data/scripts/generate_data.py

test:
	@echo "Testing DAG syntax..."
	python dags/data_pipeline.py
	@echo "DAG syntax is valid!"

db-shell:
	docker exec -it postgres psql -U airflow -d airflow

airflow-shell:
	docker exec -it airflow-webserver bash

init:
	mkdir -p data/processed logs plugins dags screenshots
	touch data/.gitkeep
	@echo "Project structure initialized"