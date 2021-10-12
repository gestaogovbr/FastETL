.PHONY: setup
setup:
	docker-compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans
	docker exec airflow sh -c "airflow db reset -y && airflow scheduler -D"

.PHONY: down
down:
	docker-compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	docker exec airflow pytest -vv
