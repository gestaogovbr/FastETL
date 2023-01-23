FROM ghcr.io/economiagovbr/airflow2-docker:latest

RUN airflow db init

ENTRYPOINT airflow webserver

EXPOSE 8080
