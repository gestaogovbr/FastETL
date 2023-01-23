.PHONY: setup
setup:
	docker-compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans
	docker exec tests_airflow sh -c "airflow db reset -y && airflow scheduler -D"

.PHONY: down
down:
	docker-compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	docker exec tests_airflow pytest -vvv
