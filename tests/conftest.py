import os
from airflow import models
from airflow.utils import db


TEST_ENV_VARS = {
    'AIRFLOW_HOME': '/opt/airflow'
}

APP_NAME = 'FastETL-plugins-tests'


def pytest_configure(config):
    """Configure and init envvars for airflow."""
    config.old_env = {}
    for key, value in TEST_ENV_VARS.items():
        config.old_env[key] = os.getenv(key)
        os.environ[key] = value
    # define some models to get the tests to pass.
    db.merge_conn(
        models.Connection(
            conn_id='postgres-source-conn', conn_type='postgres',
            host='postgres-source', schema='db',
            login='root', password='root')
    )
    db.merge_conn(
        models.Connection(
            conn_id='postgres-destination-conn', conn_type='postgres',
            host='postgres-destination', schema='db',
            login='root', password='root')
    )
    db.merge_conn(
        models.Connection(
            conn_id='postgres-destination-fake-conn', conn_type='postgres',
            host='postgres-destination', schema='db',
            login='fake', password='fake')
    )

    db.merge_conn(
        models.Connection(
            conn_id='mssql-source-conn', conn_type='mssql',
            host='mssql-source', schema='master', port=1433,
            login='sa', password='ozoBaroF2021',
            extra='{"Driver": "ODBC Driver 17 for SQL Server"}')
    )
    db.merge_conn(
        models.Connection(
            conn_id='mssql-destination-conn', conn_type='mssql',
            host='mssql-destination', schema='master', port=1433,
            login='sa', password='ozoBaroF2021',
            extra='{"Driver": "ODBC Driver 17 for SQL Server"}')
    )
    db.merge_conn(
        models.Connection(
            conn_id='mssql-destination-fake-conn', conn_type='mssql',
            host='mssql-destination', schema='master', port=1433,
            login='fake', password='fake',
            extra='{"Driver": "ODBC Driver 17 for SQL Server"}')
    )


def pytest_unconfigure(config):
    """Restore envvars to old values."""
    for key, value in config.old_env.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value
