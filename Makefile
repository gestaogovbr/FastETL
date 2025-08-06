.PHONY: setup
setup:
	docker compose -f tests/docker-compose.yml up -d --force-recreate --remove-orphans

.PHONY: down
down:
	docker compose -f tests/docker-compose.yml down

.PHONY: tests
tests:
	docker compose -f tests/docker-compose.yml exec airflow pytest -vvv --color=yes

# example: make test TEST_FILTER=test_put_participante_missing_mandatory_fields
# command `$ make test TEST_FILTER="test_put_participante_missing_mandatory_fields"`
.PHONY: test
test:
	docker compose -f tests/docker-compose.yml exec airflow pytest -k $(TEST_FILTER) -vvv --color=yes
