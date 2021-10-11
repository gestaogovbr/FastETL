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
            conn_id='pg-source', conn_type='postgresql',
            host='pg-source', schema='db',
            login='root', password='root')
    )
    db.merge_conn(
        models.Connection(
            conn_id='pg-destination', conn_type='postgresql',
            host='pg-destination', schema='db',
            login='root', password='root')
    )


def pytest_unconfigure(config):
    """Restore envvars to old values."""
    for key, value in config.old_env.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value
