"""Tests for the DbConnection and Engine functions."""

from datetime import date, datetime
import logging
import pytest
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from fastetl.custom_functions.utils.db_connection import get_mssql_odbc_engine


# Tests

def test_get_mssql_odbc_engine(
    conn_id: str
):
    conn_id = "mssql-source-conn"
    connection_url = get_mssql_odbc_engine(conn_id)

    assert connection_url == "mssql+pyodbc:"

@pytest.mark.parametrize(
    "conn_id",
    [
        "postgres",
        "mssql",
        "mysql"
    ]
)
def test_get_hook_and_engine_by_provider(
    conn_id: str
):
    pass

@pytest.mark.parametrize(
    "conn_id",
    [
        "postgres",
        "mssql",
        "mysql"
    ]
)
def test_get_get_conn_type(
    conn_id: str
):
    pass

def test_db_as_hook(
    conn_id: str
):
    pass

def test_db_as_engine(
    conn_id: str
):
    pass



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


    assert_frame_equal(source_data, dest_data)
