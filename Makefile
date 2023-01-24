.PHONY: setup
setup:
	docker-compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans
	# docker exec airflow sh -c "airflow db init"
	# docker exec airflow sh -c "airflow scheduler -D"
	# ./wait-for-airflow.sh
	# docker exec airflow sh -c "airflow db reset -y"

.PHONY: down
down:
	docker-compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	docker exec airflow sh -c "airflow db reset -y"
	docker exec airflow pytest -vvv --color=yes
