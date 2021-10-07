import pytest
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.FastETL.hooks.db_to_db_hook import DbToDbHook

def insert_data(tablename, hook, data):
    """This script will populate database with initial data to run job"""
    sample_data = pd.DataFrame(data)

    sample_data.to_sql(name=tablename,
                       con=hook.get_sqlalchemy_engine(),
                       if_exists='replace',
                       index=False)

def insert_initial_source_data(tablename, hook):
    data = {'Name':['Tom', 'nick', 'krish', 'jack'],
            'Age':[20, 21, 19, 18]}
    insert_data(tablename, hook, data)
def insert_initial_dest_empty_table(tablename, hook):
    data = {'Name':[],
            'Age':[]}
    insert_data(tablename, hook, data)

def test_replicate_table_full():
    oltp_hook = PostgresHook('oltp')
    olap_hook = PostgresHook('olap')
    insert_initial_source_data('example_table', oltp_hook)
    insert_initial_dest_empty_table('example_table', olap_hook)

    assert True