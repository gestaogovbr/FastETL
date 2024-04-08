from datetime import date, datetime
import logging
import os
import pytest
from random import randint, uniform
import subprocess

import pandas as pd
from pandas._testing import assert_frame_equal
from pyodbc import ProgrammingError
from psycopg2.errors import UndefinedTable

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook


# Constants

NAMES = ["hendrix", "nitai", "krishna", "jesus", "Danielle", "Augusto"]
DESCRIPTIONS = [
    # pylint: disable=line-too-long
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

# Auxiliary functions


def _try_drop_table(table_name: str, hook: DbApiHook) -> None:
    logging.info("Tentando apagar a tabela %s.", table_name)
    try:
        hook.run(f"DROP TABLE {table_name};")
    except (UndefinedTable, ProgrammingError) as e:
        logging.info(e)


def _create_initial_table(table_name: str, hook: DbApiHook, db_provider: str) -> None:
    filename = f"create_{table_name}_{db_provider.lower()}.sql"
    path = "/opt/airflow/tests/sql/init/"
    with open(os.path.join(path, filename), "r", encoding="utf-8") as file:
        sql_statement = file.read()
    hook.run(sql_statement.format(table_name=table_name))


def generate_transactions(
    number: int,
) -> list[tuple[int, str, str, str, int, float, date, bool, datetime]]:
    """Prepare random data for use in testing.

    Args:
        number (int): quantity of rows to return.

    Returns:
        list[tuple[int, str, str, str, int, int, date, bool, datetime]]: the
            randomized transaction data for use in testing.
    """
    transactions = []
    for x in range(0, number):
        transactions.append(
            (
                x,
                NAMES[randint(0, 3)],
                DESCRIPTIONS[randint(0, 3)],
                DESCRIPTIONS[randint(0, 3)],
                randint(1, 1000000),
                round(uniform(0, 1000), 2),
                DATETIMES[randint(0, 3)].date(),
                ACTIVES[randint(0, 1)],
                DATETIMES[randint(0, 3)],
            )
        )
    return transactions


def _insert_initial_source_table_n_data(
    table_name: str, hook: DbApiHook, db_provider: str
) -> None:
    """Insert the simulated test data into the table.

    Args:
        table_name (str): name of the table to be inserted.
        hook (DbApiHook): database hook.
        db_provider (str): name of the database provider. Must be one
            of the provided init sql files.
    """
    _create_initial_table(table_name, hook, db_provider)

    transactions_df = pd.DataFrame(
        generate_transactions(1500),
        columns=[
            "id",
            "Name",
            "Description",
            "Description2",
            "Age",
            "Weight",
            "Birth",
            "Active",
            "date_time",
        ],
    )
    transactions_df.to_sql(
        name=table_name,
        con=hook.get_sqlalchemy_engine(),
        if_exists="append",
        index=False,
    )


# Tests


@pytest.mark.parametrize(
    "source_conn_id, source_hook_cls, source_provider, dest_conn_id, "
    "dest_hook_cls, destination_provider, has_dest_table",
    [
        (
            "postgres-source-conn",
            PostgresHook,
            "postgres",
            "postgres-destination-conn",
            PostgresHook,
            "postgres",
            True,
        ),
        (
            "mssql-source-conn",
            OdbcHook,
            "mssql",
            "mssql-destination-conn",
            OdbcHook,
            "mssql",
            True,
        ),
        (
            "postgres-source-conn",
            PostgresHook,
            "postgres",
            "mssql-destination-conn",
            OdbcHook,
            "mssql",
            True,
        ),
        (
            "mssql-source-conn",
            OdbcHook,
            "mssql",
            "postgres-destination-conn",
            PostgresHook,
            "postgres",
            True,
        ),
        (
            "postgres-source-conn",
            PostgresHook,
            "postgres",
            "postgres-destination-conn",
            PostgresHook,
            "postgres",
            False,
        ),
        (
            "mssql-source-conn",
            OdbcHook,
            "mssql",
            "mssql-destination-conn",
            OdbcHook,
            "mssql",
            False,
        ),
        (
            "postgres-source-conn",
            PostgresHook,
            "postgres",
            "mssql-destination-conn",
            OdbcHook,
            "mssql",
            False,
        ),
        (
            "mssql-source-conn",
            OdbcHook,
            "mssql",
            "postgres-destination-conn",
            PostgresHook,
            "postgres",
            False,
        ),
    ],
)
def test_full_table_replication_various_db_types(
    source_conn_id: str,
    source_hook_cls: DbApiHook,
    source_provider: str,
    dest_conn_id: str,
    dest_hook_cls: DbApiHook,
    destination_provider: str,
    has_dest_table: bool,
):
    """Test full table replication using various database types.

    Args:
        source_conn_id (str): source database connection id.
        source_hook_cls (DbApiHook): source database hook class.
        source_provider (str): source database provider.
        dest_conn_id (str): destination database connection id.
        dest_hook_cls (DbApiHook): destination database hook class.
        destination_provider (str): destination database provider.
        has_dest_table (bool): whether or not to create the table at
            the destination database before testing replication.
    """
    source_table_name = "source_table"
    dest_table_name = "destination_table"
    source_hook = source_hook_cls(source_conn_id)
    dest_hook = dest_hook_cls(dest_conn_id)

    # Setup
    _try_drop_table(source_table_name, source_hook)
    _insert_initial_source_table_n_data(source_table_name, source_hook, source_provider)

    _try_drop_table(dest_table_name, dest_hook)
    if has_dest_table:
        _create_initial_table(dest_table_name, dest_hook, destination_provider)

    # Run
    task_id = f"test_from_{source_provider}_to_{destination_provider}".lower()
    subprocess.run(
        ["airflow", "tasks", "test", "test_dag", task_id, "2021-01-01"], check=True
    )

    # Assert
    source_data = source_hook.get_pandas_df(
        f"select * from {source_table_name} order by id asc;"
    )
    dest_data = dest_hook.get_pandas_df(
        f"select * from {dest_table_name} order by id asc;"
    )

    assert_frame_equal(source_data, dest_data)
