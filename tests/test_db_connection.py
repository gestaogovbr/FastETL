"""Tests for the DbConnection and Engine functions."""

from datetime import date, datetime
import logging
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
import airflow.providers.postgres.hooks.postgres as postgres
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from fastetl.custom_functions.utils.db_connection import (
    get_mssql_odbc_engine,
    get_hook_and_engine_by_provider,
    get_conn_type,
)


# Tests
@pytest.mark.parametrize(
    "conn_id", "engine_str",
    [(
        "mssql-source-conn",
        "Engine(mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BServer%3Dmssql-source%2C+1433%3B+++++++++++++++++++++Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B",
    )],
)
def test_get_mssql_odbc_engine(conn_id: str, engine_str: str):
    engine = get_mssql_odbc_engine(conn_id)
    assert (str(engine) == engine_str)


@pytest.mark.parametrize(
        "conn_id, expected_conn_type",
        [
            ("mssql-source-conn", "mssql"),
            ("postgres-source-conn", "postgres")
        ]
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
        assert isinstance(hook, postgres.PostgresHook)
        assert isinstance(
            engine, create_engine("postgresql://root:***@postgres-source/db").__class__
        )
    elif get_conn_type(conn_id) == "mssql":
        assert isinstance(hook, mssql.MsSqlHook)
        assert isinstance(
            engine,
            create_engine(
                "mssql+pyodbc://?odbc_connect=Driver%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BServer%3Dmssql-source%2C+1433%3B+++++++++++++++++++++Database%3Dmaster%3BUid%3Dsa%3BPwd%3DozoBaroF2021%3B"
            ).__class__,
        )


def test_db_as_hook(conn_id: str):
    pass


def test_db_as_engine(conn_id: str):
    pass
