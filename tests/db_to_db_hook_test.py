from datetime import datetime
import pytest
import pandas as pd
from pandas._testing import assert_frame_equal

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook

from plugins.FastETL.hooks.db_to_db_hook import DbToDbHook


def _insert_data(tablename, hook, data):
    """This script will populate database with initial data to run job"""
    sample_data = pd.DataFrame(data)

    sample_data.to_sql(name=tablename,
                       con=hook.get_sqlalchemy_engine(),
                       if_exists='replace',
                       index=False)


def _date(date_: str) -> datetime:
    return datetime.strptime(date_, '%Y-%m-%d').date()


def _insert_initial_source_data(tablename, hook):
    data = {'Name':['hendrix', 'nitai', 'krish', 'jesus'],
            'Age':[27, 38, 1000, 33],
            'Birth': [
                _date('1942-11-27'),
                _date('1983-06-02'),
                _date('3227-06-23'),
                _date('0001-12-27')]}
    _insert_data(tablename, hook, data)


def _insert_initial_dest_empty_table(tablename, hook):
    data = {'Name':['string'],
            'Age':[1],
            'Birth':[_date('0001-12-27')]}
    _insert_data(tablename, hook, data)


def test_replicate_table_full_postgres():
    source_conn_id = 'pg-source-conn'
    dest_conn_id = 'pg-destination-conn'
    table_name = 'example_table'

    source_hook = PostgresHook(source_conn_id)
    dest_hook = PostgresHook(dest_conn_id)
    _insert_initial_source_data(table_name, source_hook)
    _insert_initial_dest_empty_table(table_name, dest_hook)

    hook = DbToDbHook(
        source_conn_id=source_conn_id,
        destination_conn_id=dest_conn_id,
        source_provider='PG',
        destination_provider='PG'
        ).full_copy(
        source_table=f'public.{table_name}',
        destination_table=f'public.{table_name}',
        )

    source_data = source_hook.get_pandas_df(f'select * from {table_name}')
    dest_data = dest_hook.get_pandas_df(f'select * from {table_name}')

    assert_frame_equal(source_data, dest_data)

def test_replicate_table_full_mssql():
    source_conn_id = 'mssql-source-conn'
    dest_conn_id = 'mssql-destination-conn'
    table_name = 'example_table'

    source_hook = OdbcHook(source_conn_id)
    dest_hook = OdbcHook(dest_conn_id)
    _insert_initial_source_data(table_name, source_hook)
    _insert_initial_dest_empty_table(table_name, dest_hook)

    hook = DbToDbHook(
        source_conn_id=source_conn_id,
        destination_conn_id=dest_conn_id,
        source_provider='MSSQL',
        destination_provider='MSSQL'
        ).full_copy(
        source_table=f'dbo.{table_name}',
        destination_table=f'dbo.{table_name}',
        )

    source_data = source_hook.get_pandas_df(f'select * from {table_name}')
    dest_data = dest_hook.get_pandas_df(f'select * from {table_name}')

    assert_frame_equal(source_data, dest_data)
