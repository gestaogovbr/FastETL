.PHONY: setup
setup:
	docker compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans

.PHONY: down
down:
	docker compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	docker exec airflow pytest -vvv --color=yes
