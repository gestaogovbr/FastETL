.PHONY: setup
setup:
	docker-compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans
	# docker exec airflow sh -c "airflow db init"
	# docker exec airflow sh -c "airflow scheduler -D"

.PHONY: down
down:
	docker-compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	while [[ "$(curl --silent 'http://localhost:8080/health' | python3 -c "import sys, json; print(json.load(sys.stdin)['scheduler']['status'])")" != "healthy" ]]; do printf "."; sleep 2; done
	docker exec airflow sh -c "airflow db reset -y"
	docker exec airflow pytest -vvv --color=yes
