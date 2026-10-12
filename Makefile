# ATLAS Build & Orchestration

COMPOSE ?= docker compose

.PHONY: help build up down status logs clean

help:
	@echo "ATLAS CLI Build System"
	@echo "Usage:"
	@echo "  make build    Build all docker images"
	@echo "  make up       Start the entire stack (detached)"
	@echo "  make down     Stop all services"
	@echo "  make status   Show status of all services"
	@echo "  make logs     Tail logs from all services"
	@echo "  make clean    Remove all containers and volumes"

build:
	$(COMPOSE) -f infrastructure/docker/docker-compose.yml build

up:
	$(COMPOSE) -f infrastructure/docker/docker-compose.yml up -d

down:
	$(COMPOSE) -f infrastructure/docker/docker-compose.yml down

status:
	$(COMPOSE) -f infrastructure/docker/docker-compose.yml ps
	@atlas status

logs:
	$(COMPOSE) -f infrastructure/docker/docker-compose.yml logs -f

clean:
	$(COMPOSE) -f infrastructure/docker/docker-compose.yml down -v --rmi all
