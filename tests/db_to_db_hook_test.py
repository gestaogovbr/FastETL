from datetime import datetime, time
import logging
import pytest

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook

import pandas as pd
from pandas._testing import assert_frame_equal
from sqlalchemy import Table, Column, Integer, String, Date, Float, Boolean, MetaData
from pyodbc import ProgrammingError
from psycopg2.errors import UndefinedTable

from plugins.FastETL.hooks.db_to_db_hook import DbToDbHook


def _try_drop_table(table_name: str, hook: DbApiHook) -> None:
    logging.info('Tentando apagar a tabela %s.', table_name)
    try:
        hook.run(f'DROP TABLE {table_name};')
    except (UndefinedTable, ProgrammingError) as e:
        logging.info(e)


def _create_initial_table(table_name: str, hook: DbApiHook,
                          db_provider: str) -> None:
    filename = f'create_table_{db_provider.lower()}.sql'
    sql_stmt = open(f'/opt/airflow/tests/sql/init/{filename}').read()
    hook.run(sql_stmt.format(table_name=table_name))


def _date(date_: str) -> datetime:
    return datetime.strptime(date_, '%Y-%m-%d').date()


def _insert_initial_source_table_n_data(table_name: str, hook: DbApiHook,
                                        db_provider: str) -> None:
    _create_initial_table(table_name, hook, db_provider)
    data = {'Name':['hendrix', 'nitai', 'krishna', 'jesus'],
            'Age':[27, 38, 1000, 33],
            'Weight':[1000.0111111111111, 75.33, 333.33, 12345.54320091],
            'Birth':[
                _date('1942-11-27'),
                _date('1983-06-02'),
                _date('3227-06-23'),
                _date('0001-12-27')],
            'Active':[False, True, True, True]}

    pd.DataFrame(data).to_sql(name=table_name,
                              con=hook.get_sqlalchemy_engine(),
                              if_exists='replace',
                              index=False)


def _create_initial_dest_table(table_name: str, hook: DbApiHook,
                               db_provider: str) -> None:
    _create_initial_table(table_name, hook, db_provider)


@pytest.mark.parametrize(
    'source_conn_id, source_hook_cls, source_provider, dest_conn_id, dest_hook_cls, destination_provider, has_dest_table',
    [
        ('pg-source-conn', PostgresHook, 'PG', 'pg-destination-conn', PostgresHook, 'PG', True),
        ('mssql-source-conn', OdbcHook, 'MSSQL', 'mssql-destination-conn', OdbcHook, 'MSSQL', True),
        ('pg-source-conn', PostgresHook, 'PG', 'mssql-destination-conn', OdbcHook, 'MSSQL', True),
        ('mssql-source-conn', OdbcHook, 'MSSQL', 'pg-destination-conn', PostgresHook, 'PG', True),
        ('pg-source-conn', PostgresHook, 'PG', 'pg-destination-conn', PostgresHook, 'PG', False),
        ('mssql-source-conn', OdbcHook, 'MSSQL', 'mssql-destination-conn', OdbcHook, 'MSSQL', False),
        ('pg-source-conn', PostgresHook, 'PG', 'mssql-destination-conn', OdbcHook, 'MSSQL', False),
        ('mssql-source-conn', OdbcHook, 'MSSQL', 'pg-destination-conn', PostgresHook, 'PG', False),
    ])
def test_full_table_replication_various_db_types(
        source_conn_id: str,
        source_hook_cls: DbApiHook,
        source_provider: str,
        dest_conn_id: str,
        dest_hook_cls: DbApiHook,
        destination_provider: str,
        has_dest_table: bool):
    source_table_name = 'origin_table'
    dest_table_name = 'destination_table'
    source_hook = source_hook_cls(source_conn_id)
    dest_hook = dest_hook_cls(dest_conn_id)

    # Setup
    _try_drop_table(source_table_name, source_hook)
    _insert_initial_source_table_n_data(source_table_name,
                                        source_hook,
                                        source_provider)

    _try_drop_table(dest_table_name, dest_hook)
    if has_dest_table:
        _create_initial_dest_table(dest_table_name,
                                   dest_hook,
                                   destination_provider)

    source_schema = 'public' if source_provider == 'PG' else 'dbo'
    destination_schema = 'public' if destination_provider == 'PG' else 'dbo'

    # Run
    hook = DbToDbHook(
        source_conn_id=source_conn_id,
        destination_conn_id=dest_conn_id,
        source_provider=source_provider,
        destination_provider=destination_provider
        ).full_copy(
            source_table=f'{source_schema}.{source_table_name}',
            destination_table=f'{destination_schema}.{dest_table_name}',
        )

    # Assert
    source_data = source_hook.get_pandas_df(f'select * from {source_table_name}')
    dest_data = dest_hook.get_pandas_df(f'select * from {dest_table_name}')

    assert_frame_equal(source_data, dest_data)
