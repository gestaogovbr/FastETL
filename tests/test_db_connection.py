"""Tests for the DbConnection and Engine functions."""

from typing import Literal

import pytest
import psycopg2
import pyodbc
from sqlalchemy.engine import Engine

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from fastetl.custom_functions.utils.db_connection import (
    DbConnection,
    get_mssql_odbc_engine,
    get_hook_and_engine_by_provider,
    get_conn_type,
)

# TODO:
# - Tests for Mysql Engine and Hooks


# Tests
@pytest.mark.parametrize(
    ("conn_id", "engine_str"),
    [
        (
            "mssql-source-conn",
            "Engine(mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3B"
            "Server%3Dmssql-source%2C+1433%3B+++++++++++++++++++++"
            "Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B)",
        ),
    ],
)
def test_get_mssql_odbc_engine(conn_id: str, engine_str: str):
    """Test that the
    fastetl.custom_functions.utils.db_connection.get_mssql_odbc_engine
    function returns the correct engine for MS SQL Server, by checking
    the engine representation as a string.

    Args:
        conn_id (str): The connection id.
        engine_str (str): The expected engine string.
    """
    engine = get_mssql_odbc_engine(conn_id)
    assert str(engine) == engine_str


@pytest.mark.parametrize(
    ("conn_id, expected_conn_type"),
    [("mssql-source-conn", "mssql"), ("postgres-source-conn", "postgres")],
)
def test_get_conn_type(conn_id: str, expected_conn_type: str):
    """Test that the connection type is as expected for the given
    connections.

    Args:
        conn_id (str): The connection id.
        expected_conn_type (str): The expected connection type.
    """
    conn_type = get_conn_type(conn_id)
    assert conn_type == expected_conn_type


@pytest.mark.parametrize(
    "conn_id",
    [
        "mssql-source-conn",
        "postgres-source-conn",
    ],
)
def test_get_hook_and_engine_by_provider(conn_id: str):
    """Test that the hook and engine returned are the expected ones for
    the given connections.
    Args:
        conn_id (str): The connection id.
    """

    hook, engine = get_hook_and_engine_by_provider(conn_id)

    if get_conn_type(conn_id) == "postgres":
        assert isinstance(hook, PostgresHook)
        assert str(engine.url) == "postgresql://root:root@postgres-source/db"

    elif get_conn_type(conn_id) == "mssql":
        assert isinstance(hook, MsSqlHook)
        assert str(engine.url) == (
            "mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3B"
            "Server%3Dmssql-source%2C+1433%3B+++++++++++++++++++++"
            "Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B"
        )


@pytest.mark.parametrize(
    ("conn_id", "use"),
    [
        ("mssql-source-conn", "hook"),
        ("postgres-source-conn", "hook"),
        ("mssql-source-conn", "connection"),
        ("postgres-source-conn", "connection"),
        ("mssql-source-conn", "engine"),
        ("postgres-source-conn", "engine"),
    ],
)
def test_db_connection_object_type(
    conn_id: str,
    use: Literal["hook", "connection", "engine"],
):
    """Test that the DbConnection returns the appropriate type of object.

    Args:
        conn_id (str): The connection id.
        use (Literal["hook", "connection", "engine"]): What object to test.
    """
    CONN_CLASS = {
        "postgres": psycopg2.extensions.connection,
        "mssql": pyodbc.Connection,
    }
    with DbConnection(conn_id=conn_id, use=use) as db_object:
        if use == "hook":
            assert isinstance(db_object, BaseHook)
        elif use == "connection":
            conn_type = get_conn_type(conn_id)
            assert isinstance(
                db_object, CONN_CLASS[conn_type]
            )
        else:
            assert isinstance(db_object, Engine)


@pytest.mark.parametrize(
    "conn_id",
    [
        "mssql-source-fake-conn",
        "postgres-source-fake-conn",
    ],
)
def test_db_fail_connection_wrong_credentials(conn_id: str):
    """Test that the DbConnection will fail for a connection that is using
    wrong credentials.

    Args:
        conn_id (str): The connection id.
    """
    with pytest.raises((IOError, psycopg2.OperationalError)):
        with DbConnection(conn_id=conn_id):
            pass
