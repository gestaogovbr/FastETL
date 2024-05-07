"""Tests for the DbConnection and Engine functions."""

import pytest
import psycopg2
import pyodbc
from sqlalchemy import create_engine

import airflow.providers.postgres.hooks.postgres as postgres

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from fastetl.custom_functions.utils.db_connection import (
    DbConnection,
    get_mssql_odbc_engine,
    get_hook_and_engine_by_provider,
    get_conn_type,
)

#TODO:
# - Tests for Mysql Engine and Hooks

# Tests
@pytest.mark.parametrize(
    ("conn_id", "engine_str"),
    [
        ("mssql-source-conn", "Engine(mssql+pyodbc:"),
    ],
)
def test_get_mssql_odbc_engine(conn_id: str, engine_str: str):
    engine = get_mssql_odbc_engine(conn_id)
    assert str(engine) == \
    "Engine(mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BServer%3D" + \
        "mssql-source%2C+1433%3B+++++++++++++++++++++Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B)"


@pytest.mark.parametrize(
    ("conn_id, expected_conn_type"),
    [("mssql-source-conn", "mssql"), ("postgres-source-conn", "postgres")],
)
def test_get_conn_type(conn_id: str, expected_conn_type: str):
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
    hook, engine = get_hook_and_engine_by_provider(conn_id)
    if get_conn_type(conn_id) == "postgres":
        assert isinstance(hook, PostgresHook)
        assert str(engine.url) == "postgresql://root:root@postgres-source/db"

    elif get_conn_type(conn_id) == "mssql":
        assert isinstance(hook, MsSqlHook)
        assert str(engine.url) == \
            "mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BServer%3D" + \
            "mssql-source%2C+1433%3B+++++++++++++++++++++Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B"


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
def test_db_connection(conn_id: str, use: str):

    with DbConnection(conn_id=conn_id, use=use) as db_hook:
        if get_conn_type(conn_id) == "postgres":
            assert (
                isinstance(db_hook, PostgresHook)
                or isinstance(db_hook, psycopg2.extensions.connection)
                or str(db_hook) == "Engine(postgresql://root:***@postgres-source/db)"
            )

        if get_conn_type(conn_id) == "mssql":
            assert (
                isinstance(db_hook, MsSqlHook)
                or isinstance(db_hook, pyodbc.Connection)
                or str(db_hook) == \
                "Engine(mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BServer%3D" + \
                "mssql-source%2C+1433%3B+++++++++++++++++++++Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B)"
            )
