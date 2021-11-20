from datetime import datetime, date
import logging
import subprocess
import pytest

from random import sample, randint, uniform

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow import settings
from airflow.models import Connection

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
    filename = f'create_{table_name}_{db_provider.lower()}.sql'
    sql_stmt = open(f'/opt/airflow/tests/sql/init/{filename}').read()
    hook.run(sql_stmt.format(table_name=table_name))

def _get_conn_password(conn_id: str) -> str:
    "Gets the password from an Airflow connection."
    session = settings.Session()
    conn = session.query(Connection).filter_by(conn_id=conn_id).one()
    return conn.password

def _set_conn_password(conn_id: str, password: str) -> None:
    "Sets the password of an Airflow connection."
    session = settings.Session()
    conn = session.query(Connection).filter_by(conn_id=conn_id).one()
    conn.password = password
    session.commit()

NAMES = ['hendrix', 'nitai', 'krishna', 'jesus', 'Danielle', 'Augusto']
DESCRIPTIONS = [
    "Eh um fato conhecido de todos que um leitor se distrairá com o conteúdo de texto legível de uma página quando estiver examinando sua diagramação. A vantagem de usar Lorem Ipsum é que ele tem uma distribuição normal de letras, ao contrário de Conteúdo aqui, conteúdo aqui, fazendo com que ele tenha uma aparência similar a de um texto legível.",
    "Muitos softwares de publicação e editores de páginas na internet agora usam Lorem Ipsum como texto-modelo padrão, e uma rápida busca por 'lorem ipsum' mostra vários websites ainda em sua fase de construção. Várias versões novas surgiram ao longo dos anos, eventualmente por acidente, e às vezes de propósito",
    "Existem muitas variações disponíveis de passagens de Lorem Ipsum, mas a maioria sofreu algum tipo de alteração, seja por inserção de passagens com humor, ou palavras aleatórias que não parecem nem um pouco convincentes. Se você pretende usar uma passagem de Lorem Ipsum, precisa ter certeza de que não há algo embaraçoso escrito ",
    "Ao contrário do que se acredita, Lorem Ipsum não é simplesmente um texto randômico. Com mais de 2000 anos, suas raízes podem ser encontradas em uma obra de literatura latina clássica datada de 45 AC. Richard McClintock, um professor de latim do Hampden-Sydney College na Virginia, pesquisou uma das mais obscuras palavras em latim, consectetur, oriunda de uma passagem de Lorem Ipsum, e, procurando por entre citações da palavra na literatura clássica, descobriu a sua",
]
DATETIMES = [
    datetime(1942, 11, 27, 1, 2, 3),
    datetime(1983, 6, 2, 1, 2, 3),
    datetime(3227, 6, 23, 1, 2, 3),
    datetime(1, 12, 27, 1, 2, 3),
]
ACTIVES = [True, False]


def generate_transactions(number):
    transactions = []
    for x in range(0, number):
        transactions.append((x,
                             NAMES[randint(0, 3)],
                             DESCRIPTIONS[randint(0, 3)],
                             DESCRIPTIONS[randint(0, 3)],
                             randint(1, 1000000),
                             round(uniform(0, 1000), 2),
                             DATETIMES[randint(0, 3)].date(),
                             ACTIVES[randint(0, 1)],
                             DATETIMES[randint(0, 3)]))
    return transactions


def _insert_initial_source_table_n_data(table_name: str, hook: DbApiHook,
                                        db_provider: str) -> None:
    _create_initial_table(table_name, hook, db_provider)

    transactions_df = pd.DataFrame(generate_transactions(1500),
                                   columns=[
                                       'id',
                                       'Name',
                                       'Description',
                                       'Description2',
                                       'Age',
                                       'Weight',
                                       'Birth',
                                       'Active',
                                       'date_time'])
    transactions_df.to_sql(name=table_name,
                           con=hook.get_sqlalchemy_engine(),
                           if_exists='append',
                           index=False)


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
    source_table_name = 'source_table'
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
        _create_initial_table(dest_table_name,
                              dest_hook,
                              destination_provider)

    # Run
    task_id = f'test_from_{source_provider}_to_{destination_provider}'.lower()
    subprocess.run(
        ['airflow', 'tasks', 'test', 'test_dag', task_id, '2021-01-01'],
        check=True)

    # Assert
    source_data = source_hook.get_pandas_df(f'select * from {source_table_name} order by id asc;')
    dest_data = dest_hook.get_pandas_df(f'select * from {dest_table_name} order by id asc;')

    assert_frame_equal(source_data, dest_data)

@pytest.mark.parametrize(
    'dest_conn_id, dest_fake_conn_id, dest_hook_cls, destination_provider',
    [
        ('pg-destination-conn', 'pg-destination-fake-conn', PostgresHook,
            'PG'),
        ('mssql-destination-conn', 'mssql-destination-fake-conn', OdbcHook,
            'MSSQL'),
    ])
def test_breaks_with_wrong_credentials(
        dest_conn_id: str,
        dest_fake_conn_id: str,
        dest_hook_cls: DbApiHook,
        destination_provider: str):
    ERROR_LOGIN = {
        'PG': 'password authentication failed for user',
        'MSSQL': 'Login failed for user',
    }
    source_table_name = 'source_table'
    dest_table_name = 'destination_table'
    source_conn_id = 'pg-source-conn'
    source_hook_cls = PostgresHook
    source_provider = 'PG'
    source_hook = source_hook_cls(source_conn_id)
    dest_hook = dest_hook_cls(dest_conn_id)

    # Setup
    _try_drop_table(source_table_name, source_hook)
    _insert_initial_source_table_n_data(source_table_name,
                                        source_hook,
                                        source_provider)
    #   the same data will already exist on the destination
    _try_drop_table(dest_table_name, dest_hook)
    _insert_initial_source_table_n_data(dest_table_name,
                                        dest_hook,
                                        destination_provider)
    #   change the connection passwords so that it will be wrong
    original_password = _get_conn_password(dest_conn_id)
    fake_password = _get_conn_password(dest_fake_conn_id)
    _set_conn_password(dest_conn_id, fake_password)

    # Run
    task_id = f'test_from_{source_provider}_to_{destination_provider}'.lower()
    # should raise an exception for invalid credentials
    process = subprocess.run(
        ['airflow', 'tasks', 'test', 'test_dag', task_id, '2021-01-01'],
        capture_output=True)

    # Tear down
    _set_conn_password(dest_conn_id, original_password)

    # Check
    stderr = str(process.stderr)
    logging.info('Task error output: %s', stderr)
    assert ERROR_LOGIN[destination_provider] in stderr
