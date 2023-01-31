"""
Class and functions to connect mssql, postgres and mysql databases with
airflow hooks, sqlalchemy and pyodbc.
"""

from typing import Tuple
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
import pyodbc

from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


class DbConnection:
    """
    Gera as conexões origem e destino dependendo do tipo de provider.
    Providers disponíveis: 'mssql', 'postgres' e 'mysql'
    """

    def __init__(self, conn_id: str):
        self.conn_type = get_conn_type(conn_id)
        self.conn = None

        if self.conn_type == "mssql":
            self.mssql_conn_string = get_mssql_odbc_conn_str(
                conn_id=conn_id, raw_str=True
            )
        else:
            self.hook, _ = get_hook_and_engine_by_provider(conn_id)

    def __enter__(self):
        if self.conn_type == "mssql":
            try:
                self.conn = pyodbc.connect(self.mssql_conn_string)
            except Exception as exc:
                raise Exception(f"{self.conn_type} connection failed.") from exc
        else:
            try:
                self.conn = self.hook.get_conn()
            except Exception as exc:
                raise Exception(f"{self.conn_type} connection failed.") from exc

        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def get_mssql_odbc_conn_str(conn_id: str, raw_str: bool = False) -> str:
    """
    Creates a default SQL Server database connection string
    for pyodbc or simple raw string.

    Args:
        conn_id(str): Airflow database connection id.
        raw_str(bool): Flag to return raw string or pyodbc formatted
            string.

    Returns:
        str: raw connection string or pyodbc formatted string.
    """

    conn_values = BaseHook.get_connection(conn_id)
    driver = "{ODBC Driver 17 for SQL Server}"
    server = conn_values.host
    port = conn_values.port
    database = conn_values.schema
    user = conn_values.login
    password = conn_values.password

    mssql_conn_str = f"""Driver={driver};Server={server}, {port}; \
                    Database={database};Uid={user};Pwd={password};"""

    if raw_str:
        return mssql_conn_str

    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": mssql_conn_str})

    return connection_url


def get_mssql_odbc_engine(conn_id: str):
    """
    Cria uma engine de conexão com banco SQL Server usando driver pyodbc.
    """

    return create_engine(get_mssql_odbc_conn_str(conn_id))


def get_hook_and_engine_by_provider(conn_id: str) -> Tuple[DbApiHook, Engine]:
    """
    Creates connection hook and engine by connection type/provider.
    Works for mssql, postgres and mysql.

    Args:
        conn_id (str): Airflow connection id.

    Returns:
        Tuple[DbApiHook, Engine]: Connection hook and engine.
    """

    conn_type = get_conn_type(conn_id)

    if conn_type == "mssql":
        hook = MsSqlHook(conn_id)
        engine = get_mssql_odbc_engine(conn_id)
    elif conn_type == "postgres":
        hook = PostgresHook(conn_id)
        engine = hook.get_sqlalchemy_engine()
    elif conn_type == "mysql":
        hook = MySqlHook(conn_id)
        engine = hook.get_sqlalchemy_engine()
    else:
        raise Exception(f"Connection type {conn_type} not implemented")

    return hook, engine


def get_conn_type(conn_id: str) -> str:
    """Get connection type from Airflow connections.

    Args:
        conn_id (str): Airflow connection id.

    Returns:
        str: type of connection. Ex: mssql, postgres, ...
    """

    conn_values = BaseHook.get_connection(conn_id)
    conn_type = conn_values.conn_type

    return conn_type
