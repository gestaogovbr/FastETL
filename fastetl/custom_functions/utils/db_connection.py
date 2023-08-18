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
                raise Exception(
                    f"{self.conn_type} connection failed."
                ) from exc
        else:
            try:
                self.conn = self.hook.get_conn()
            except Exception as exc:
                raise Exception(
                    f"{self.conn_type} connection failed."
                ) from exc

        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


class SourceConnection:
    """Represents a source connection to a database, encapsulating the
    connection details (e.g., connection ID, schema, table, query)
    required to read data from a database.

    Args:
        conn_id (str): The unique identifier of the connection to use.
        schema (str, optional): The name of the schema to use.
            Default is None.
        table (str, optional): The name of the table to use.
            Default is None.
        query (str, optional): The SQL query to use. Default is None.

    Raises:
        ValueError: If `conn_id` is empty or if neither `query` nor
            (`schema` and `table`) is provided.

    Attributes:
        conn_id (str): The unique identifier of the connection.
        schema (str): The name of the schema.
        table (str): The name of the table.
        query (str): The SQL query.
        conn_type (str): Connection type/provider.
    """

    def __init__(
        self,
        conn_id: str,
        schema: str = None,
        table: str = None,
        query: str = None,
    ):
        if not conn_id:
            raise ValueError("conn_id argument cannot be empty")
        if not query and not (schema or table):
            raise ValueError("must provide either schema and table or query")

        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.query = query
        self.conn_type = get_conn_type(conn_id)
        conn_values = BaseHook.get_connection(conn_id)
        self.conn_database = conn_values.schema


class DestinationConnection:
    """Represents a destination connection to a database, encapsulating
    the connection details (e.g., connection ID, schema, table) required
    to write data to a database.

    Args:
        conn_id (str): The unique identifier of the connection to use.
        schema (str): The name of the schema to use.
        table (str): The name of the table to use.

    Attributes:
        conn_id (str): The unique identifier of the connection.
        schema (str): The name of the schema.
        table (str): The name of the table.
        conn_type (str): Connection type/provider.
    """

    def __init__(self, conn_id: str, schema: str, table: str):
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.conn_type = get_conn_type(conn_id)
        conn_values = BaseHook.get_connection(conn_id)
        self.conn_database = conn_values.schema


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

    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": mssql_conn_str}
    )

    return connection_url


def get_mssql_odbc_engine(conn_id: str, **kwargs):
    """
    Cria uma engine de conexão com banco SQL Server usando driver pyodbc.
    """

    return create_engine(get_mssql_odbc_conn_str(conn_id), **kwargs)

 
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
    elif conn_type == "postgres" or conn_type == "teiid":
        hook = PostgresHook(conn_id)
        engine = hook.get_sqlalchemy_engine()
    elif conn_type == "mysql":
        hook = MySqlHook(conn_id)
        engine = hook.get_sqlalchemy_engine()
    else:
        raise ValueError(f"Connection type {conn_type} not implemented")

    return hook, engine


def get_conn_type(conn_id: str) -> str:
    """Get connection type from Airflow connections.

    Args:
        conn_id (str): Airflow connection id.

    Returns:
        str: type of connection. Ex: mssql, postgres, teiid, ...
    """

    conn_values = BaseHook.get_connection(conn_id)
    conn_type = (
        "teiid"
        if conn_values.description and "teiid" in conn_values.description.lower()
        else conn_values.conn_type
    )

    return conn_type
