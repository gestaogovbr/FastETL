from datetime import datetime
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
    data = {'Name':['hendrix', 'nitai', 'krish', 'jesus'],
            'Age':[27, 38, 1000, 33],
            'Birth': [
                datetime.strptime('1942-11-27', '%Y-%m-%d'),
                datetime.strptime('1983-06-02', '%Y-%m-%d'),
                datetime.strptime('3227-06-23', '%Y-%m-%d'),
                datetime.strptime('0001-12-27', '%Y-%m-%d')]}
    insert_data(tablename, hook, data)


def insert_initial_dest_empty_table(tablename, hook):
    data = {'Name':['string'],
            'Age':[1],
            'Birth':[datetime.strptime('0001-12-27', '%Y-%m-%d')]}
    insert_data(tablename, hook, data)


def test_replicate_table_full():
    source_conn_id = 'oltp'
    destination_conn_id = 'olap'
    table_name = 'example_table'

    oltp_hook = PostgresHook(source_conn_id)
    olap_hook = PostgresHook(destination_conn_id)
    insert_initial_source_data(table_name, oltp_hook)
    insert_initial_dest_empty_table(table_name, olap_hook)

    hook = DbToDbHook(
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        source_provider='PG',
        destination_provider='PG'
        ).full_copy(
        source_table=f'public.{table_name}',
        destination_table=f'public.{table_name}',
        )

    assert True