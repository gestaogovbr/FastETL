from datetime import datetime
import pytest
import pandas as pd
from pandas._testing import assert_frame_equal

from airflow.hooks.base_hook import BaseHook
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


def _create_initial_table(tablename: str, hook: DbApiHook) -> None:
    meta = MetaData()

    test_table = Table(
        tablename, meta,
        Column('Name', String),
        Column('Age', Integer),
        Column('Birth', Date)
    )
    meta.create_all(hook.get_sqlalchemy_engine())


def _insert_initial_source_data(tablename, hook):
    _create_initial_table(table_name, hook)
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


@pytest.mark.parametrize(
    'source_conn_id, source_hook_cls, source_provider, dest_conn_id, dest_hook_cls, destination_provider',
    [
        ('pg-source-conn', PostgresHook, 'PG', 'pg-destination-conn', PostgresHook, 'PG'),
        ('mssql-source-conn', OdbcHook, 'MSSQL', 'mssql-destination-conn', OdbcHook, 'MSSQL'),
        ('pg-source-conn', PostgresHook, 'PG', 'mssql-destination-conn', OdbcHook, 'MSSQL'),
        ('mssql-source-conn', OdbcHook, 'MSSQL', 'pg-destination-conn', PostgresHook, 'PG'),
    ])
def test_full_table_replication_various_db_types(
    source_conn_id: str,
    source_hook_cls: BaseHook,
    source_provider: str,
    dest_conn_id: str,
    dest_hook_cls: BaseHook,
    destination_provider: str):

    table_name = 'example_table'
    source_hook = source_hook_cls(source_conn_id)
    dest_hook = dest_hook_cls(dest_conn_id)
    _insert_initial_source_data(table_name, source_hook)
    _create_initial_table(table_name, dest_hook)

    source_schema = 'public' if source_provider == 'PG' else 'dbo'
    destination_schema = 'public' if destination_provider == 'PG' else 'dbo'

    hook = DbToDbHook(
        source_conn_id=source_conn_id,
        destination_conn_id=dest_conn_id,
        source_provider=source_provider,
        destination_provider=destination_provider
        ).full_copy(
        source_table=f'{source_schema}.{table_name}',
        destination_table=f'{destination_schema}.{table_name}',
        )

    source_data = source_hook.get_pandas_df(f'select * from {table_name}')
    dest_data = dest_hook.get_pandas_df(f'select * from {table_name}')

    assert_frame_equal(source_data, dest_data)
