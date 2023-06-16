"""
Create new table on the destination database based on the source database
table layout.

Works from:
    - postgres
    - teiid
    - mssql
To:
    - postgres
    - mssql
"""

import os
import logging
import yaml
import pandas as pd
import psycopg2
import pymssql

from sqlalchemy import Table, Column, MetaData
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes as sa_types
import sqlalchemy.dialects as sa_dialects

from airflow.hooks.base import BaseHook

from fastetl.custom_functions.utils.db_connection import (
    SourceConnection,
    DestinationConnection,
    get_hook_and_engine_by_provider,
)


def _execute_query(conn_id, query):
    """Executes a SQL query using the specified database connection.

    Args:
        conn_id (str): The connection ID or name of the database connection.
        query (str): The SQL query to execute.

    Raises:
        Exception: If there is an error while executing the query.
    """

    conn = BaseHook.get_connection(conn_id)
    hook = conn.get_hook()
    hook.run(query)


def _create_table_ddl(destination: DestinationConnection, df: pd.DataFrame):
    """Generates a Data Definition Language (DDL) query to create a
    table based on a pandas DataFrame.

    Args:
        destination (DestinationConnection): The destination database
            connection object containing information about the schema
            and table where the table will be created.
        df (pd.DataFrame): The pandas DataFrame containing the column
            information for the table.

    Returns:
        str: The DDL query to create the table.
    """

    sql_columns = []
    for _, row in df.iterrows():
        sql_columns.append(
            f"{row['Name']} {row['DataType']}{row['converted_length']}"
        )

    query = (
        f"CREATE TABLE {destination.schema}.{destination.table} ("
        f"{', '.join(sql_columns)}"
        ");"
    )

    return query


def _convert_datatypes(
    row: pd.Series,
    types_mapping: dict,
    source_conn_type: str,
    destination_conn_type: str,
) -> pd.Series:
    """Convert row(pd.Series) columns `DataType` and `converted_length`
    based on mapped information (from-to) at `types_mapping` dictionary.

    Args:
        row (pd.Series): table column metadata.
        types_mapping (dict): dictionary with database columns metadata
            (datatypes).
        source_conn_type (str): source table database connection type,
            as `postgres`, `mssql` or `teiid`.
        destination_conn_type (str): destination table database
            connection type, as `postgres`, `mssql` or `teiid`.

    Returns:
        pd.Series: updated/converted row columns `DataType` and
            `converted_length` values.
    """

    if row["DataType"] in types_mapping[source_conn_type]:
        types_node = types_mapping[source_conn_type][row["DataType"]][
            destination_conn_type
        ]

        if "IsLengthFixed" in types_node:
            row["DataType"] = types_node["dtype"][row["IsLengthFixed"]]
        else:
            row["DataType"] = types_node["dtype"]

        if "length_columns" in types_node:
            length_columns = types_node["length_columns"]
            values = [str(row[key]) for key in length_columns]
            row["converted_length"] = f"({','.join(values)})"

            if "max_length" in types_node:
                max_length, mapped_length = next(
                    iter(types_node["max_length"].items())
                )
                # uses only the first length_column of a list to compare
                # with column max_length
                if row[length_columns[0]] >= max_length:
                    row["converted_length"] = f"({mapped_length})"

    return row


def _load_yaml(file_name: str) -> dict:
    """Loads a YAML file and returns its contents as a dictionary.

    Args:
        file_name (str): The name of the YAML file to load.

    Returns:
        dict: A dictionary containing the contents of the YAML file.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        yaml.YAMLError: If there is an error while parsing the YAML file.
    """

    current_path = os.path.dirname(__file__)
    yaml_dict = yaml.safe_load(
        open(
            os.path.join(current_path, file_name),
            encoding="utf-8",
        )
    )

    return yaml_dict


def _get_teiid_columns_datatype(source: SourceConnection) -> pd.DataFrame:
    """Retrieves table columns information with data types from a Teiid
    source database.

    Args:
        source (SourceConnection): A `SourceConnection` object containing
            the connection details for the source database.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved column
            information.
    """

    conn = BaseHook.get_connection(source.conn_id)
    hook = conn.get_hook()

    rows = hook.get_pandas_df(
        f"""SELECT
                TableName,
                Name,
                DataType,
                Scale,
                Length,
                IsLengthFixed,
                "Precision",
                Description
            FROM
                SYS.Columns
            WHERE
                VDBName = '{source.conn_database}'
                and SchemaName = '{source.schema}'
                and TableName IN ('{source.table}')
        """
    )

    rows.replace({'"': "", "'": ""}, regex=True, inplace=True)

    return rows


def create_table_from_teiid(
    source: SourceConnection, destination: DestinationConnection
):
    """Create table at destination database when the source database
    conn_type is `teiid`.

    Args:
        source (SourceConnection): A `SourceConnection` object containing
            the connection details for the source database.
        destination (DestinationConnection): A `DestinationConnection`
            object containing the connection details for the destination
            database.
    """

    df_source_columns = _get_teiid_columns_datatype(source)
    df_source_columns["converted_length"] = ""
    types_mapping = _load_yaml("config/types_mapping.yml")
    df_destination_columns = df_source_columns.apply(
        _convert_datatypes,
        args=(
            types_mapping,
            source.conn_type,
            destination.conn_type,
        ),
        axis=1,
    )
    table_ddl = _create_table_ddl(destination, df_destination_columns)
    _execute_query(destination.conn_id, table_ddl)


def create_table_from_others(
    source: SourceConnection, destination: DestinationConnection
):
    """Creates a destination table if it does not already exist and copies
    data from a source table to the destination. Works only with postgres
    and mssql on source.

    Args:
        source (SourceConnection): A `SourceConnection` object containing
            the connection details for the source database.
        destination (DestinationConnection): A `DestinationConnection`
            object containing the connection details for the destination
            database.
    Returns:
        None.
    Raises:
        DatabaseError: If there is an error with the database connection or
            query.
        OperationalError: If there is an error with the database operation.
        NoSuchModuleError: If a required module is missing.
    """

    def _convert_column(old_col: Column, db_provider: str):
        """Convert column type.

        Args:
            old_col (Column): Column to convert type.
            db_provider (str): Connection type. If `mssql` or `postgres`.
        """

        type_mapping = {
            "NUMERIC": sa_types.Numeric(38, 13),
            "BIT": sa_types.Boolean(),
        }

        if db_provider == "mssql":
            type_mapping["DATETIME"] = sa_dialects.mssql.DATETIME2()

        return Column(
            old_col["name"],
            type_mapping.get(
                str(old_col["type"]._type_affinity()),
                old_col["type"]._type_affinity(),
            ),
        )

    # Table does not exist so we create it
    _, source_eng = get_hook_and_engine_by_provider(source.conn_id)
    _, destination_eng = get_hook_and_engine_by_provider(destination.conn_id)
    source_eng.echo = True
    try:
        insp = reflection.Inspector.from_engine(source_eng)
    except AssertionError as e:  # pylint: disable=invalid-name
        logging.error(
            "Cannot create the table automatically from this database."
            "Please create the table manually to execute data copying."
        )
        raise e

    generic_columns = insp.get_columns(source.table, source.schema)
    dest_columns = [
        _convert_column(c, destination.conn_type) for c in generic_columns
    ]

    destination_meta = MetaData(bind=destination_eng)
    Table(
        destination.table,
        destination_meta,
        *dest_columns,
        schema=destination.schema,
    )

    destination_meta.create_all(destination_eng)


def _check_if_table_exists(destination: DestinationConnection) -> bool:
    """Checks if a table exists in the specified database connection.

    Args:
        destination (DestinationConnection): A `DestinationConnection`
            object containing the connection details for the destination
            database.

    Returns:
        bool: True if the table exists, False otherwise.

    Raises:
        Exception: If there is an error while checking for table existence.
    """

    conn = BaseHook.get_connection(destination.conn_id)
    hook = conn.get_hook()

    try:
        # Query to check if the table exists
        query = f"""
            SELECT *
            FROM {destination.schema}.{destination.table}
            WHERE 1=2
        """
        hook.get_first(query)
        return True
    except ( # pylint: disable=invalid-name
        psycopg2.errors.UndefinedTable,
        pymssql._pymssql.ProgrammingError,
    ) as e:
        logging.info(
            "Table `%s.%s` does not exists at connection `%s`. Exception: %s",
            destination.schema,
            destination.table,
            destination.conn_id,
            e,
        )
        return False


def create_table_if_not_exists(
    source: SourceConnection, destination: DestinationConnection
):
    """Create table at destination database based on source database table
    if it not exsists already.

    Args:
        source (SourceConnection): A `SourceConnection` object containing
            the connection details for the source database.
        destination (DestinationConnection): A `DestinationConnection`
            object containing the connection details for the destination
            database.

    To Do:
        * Refactor function `create_table_from_teiid(source, destination)`
        to implement `create_table_from_others(source, destination)`
        scenarios (source table from databases mssql and postgres)
    """

    if not _check_if_table_exists(destination):
        if source.conn_type == "teiid":
            create_table_from_teiid(source, destination)
        else:
            create_table_from_others(source, destination)
