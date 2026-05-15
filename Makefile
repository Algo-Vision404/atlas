# ATLAS Build & Orchestration

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
	docker-compose -f infrastructure/docker/docker-compose.yml build

up:
	docker-compose -f infrastructure/docker/docker-compose.yml up -d

down:
	docker-compose -f infrastructure/docker/docker-compose.yml down

status:
	docker-compose -f infrastructure/docker/docker-compose.yml ps
	atlas status

logs:
	docker-compose -f infrastructure/docker/docker-compose.yml logs -f

clean:
	docker-compose -f infrastructure/docker/docker-compose.yml down -v --rmi all
