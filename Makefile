.PHONY: setup
setup:
	docker-compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans
	echo 'Wait Airflow to init.'
	sleep 60

.PHONY: down
down:
	docker-compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	docker exec airflow pytest -vv
