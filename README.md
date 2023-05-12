![FastETL's logo. It's a Swiss army knife with some open tools](docs/images/logo.svg)

<p align="center">
    <em>FastETL framework, modern, versatile, does almost everything.</em>
</p>

Este texto também está disponível em português: 🇧🇷[LEIAME.md](LEIAME.md).

---

[![CI Tests](https://github.com/gestaogovbr/FastETL/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/gestaogovbr/FastETL/actions/workflows/ci-tests.yml)

**FastETL** is a plugins package for Airflow for building data pipelines
for a number of common scenarios.

Main features:
* Full or incremental **replication** of tables in SQL Server, Postgres
  and MySQL databases
* Load data from **GSheets** and from spreadsheets on **Samba/Windows**
  networks
* Extracting **CSV** from SQL
* Clean data using custom data patching tasks (e.g. for messy
  geographical coordinates, mapping canonical values for columns, etc.)
* Querying the Brazilian National Official Gazette's (**DOU**'s) API
* Using a [Open Street Routing Machine](https://project-osrm.org/)
  service to calculate route distances
* Using [CKAN](https://docs.ckan.org/en/2.10/api/index.html) or
  dados.gov.br's API to update dataset metadata
* Using Frictionless
  [Tabular Data Packages](https://specs.frictionlessdata.io/tabular-data-package/)
  to write OpenDocument Text format data dictionaries

<!-- Contar a história da origem do FastETL -->
This framework is maintained by a network of developers from many teams
at the Ministry of Management and Innovation in Public Services and is
the cumulative result of using
[Apache Airflow](https://airflow.apache.org/), a free and open source
tool, starting in 2019.

**For government:** FastETL is widely used for replication of data queried
via Quartzo (DaaS) from Serpro.

# Installation in Airflow

FastETL implements the standards for Airflow plugins. To install it,
simply add the `apache-airflow-providers-fastetl` package to your
Python dependencies in your Airflow environment.

Or install it with

```bash
pip install apache-airflow-providers-fastetl
```

To see an example of an Apache Airflow container that uses FastETL,
check out the
[airflow2-docker](https://github.com/gestaogovbr/airflow2-docker)
repository.

To ensure appropriate results, please make sure to install the
`msodbcsql17` and `unixodbc-dev` libraries on your Apache Airflow workers.

# Tests

The test suite uses Docker containers to simulate a complete use
environment, including Airflow and the databases. For that reason, to
execute the tests, you first need to install Docker and docker-compose.

For people using Ubuntu 20.04, you can just type on the terminal:

```bash
snap install docker
```

For other versions and operating systems, see the
[official Docker documentation](https://docs.docker.com/get-docker/).


To build the containers:

```bash
make setup
```

To run the tests, use:

```bash
make setup && make tests
```

To shutdown the environment, use:

```bash
make down
```

# Usage examples

The main FastETL feature is the `DbToDbOperator` operator. It copies data
between `postgres` and `mssql` databases. MySQL is also supported as a
source.

Here goes an example:

```python
from datetime import datetime
from airflow import DAG
from fastetl.operators.db_to_db_operator import DbToDbOperator

default_args = {
    "start_date": datetime(2023, 4, 1),
}

dag = DAG(
    "copy_db_to_db_example",
    default_args=default_args,
    schedule_interval=None,
)


t0 = DbToDbOperator(
    task_id="copy_data",
    source={
        "conn_id": airflow_source_conn_id,
        "schema": source_schema,
        "table": table_name,
    },
    destination={
        "conn_id": airflow_dest_conn_id,
        "schema": dest_schema,
        "table": table_name,
    },
    destination_truncate=True,
    copy_table_comments=True,
    chunksize=10000,
    dag=dag,
)
```

More detail about the parameters and the workings of `DbToDbOperator`
can bee seen on the following files:

* [fast_etl.py](fastetl/custom_functions/fast_etl.py)
* [db_to_db_operator.py](fastetl/operators/db_to_db_operator.py)

# How to contribute

To be written on the `CONTRIBUTING.md` document (issue
[#4](/gestaogovbr/FastETL/issues/4)).
