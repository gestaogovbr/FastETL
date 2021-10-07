import pytest
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.FastETL.hooks.db_to_db_hook import DbToDbHook

def insert_initial_data(tablename, hook):
    """This script will populate database with initial data to run job"""
    conn_engine = hook.get_sqlalchemy_engine()

    data = {'Name':['Tom', 'nick', 'krish', 'jack'],
            'Age':[20, 21, 19, 18]}
    sample_data = pd.DataFrame(data)

    sample_data.to_sql(name=tablename,
                       con=conn_engine,
                       if_exists='replace',
                       index=False)

def test_replicate_table_full():
    oltp_hook = PostgresHook('oltp')
    insert_initial_data('source_table', oltp_hook)

    assert True