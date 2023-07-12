# Thanks to Jedi Wash! *.*
"""
Copy tabular data between Postgres, MSSQL and MySQL.
"""

import time
from datetime import datetime, date
import re
from typing import Union, Tuple, Dict
import logging
import pandas as pd
import psycopg2

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from fastetl.custom_functions.utils.db_connection import (
    DbConnection,
    SourceConnection,
    DestinationConnection,
    get_conn_type,
    get_mssql_odbc_engine,
    get_hook_and_engine_by_provider,
)
from fastetl.custom_functions.utils.load_info import LoadInfo
from fastetl.custom_functions.utils.table_comments import TableComments
from fastetl.custom_functions.utils.get_table_cols_name import (
    get_table_cols_name,
)
from fastetl.custom_functions.utils.create_table import create_table_if_not_exists


def build_select_sql(schema: str, table: str, column_list: str) -> str:
    """Generates sql `select` query based on schema, table and columns."""

    columns = ", ".join(f'"{col}"' for col in column_list)

    return f"SELECT {columns} FROM {schema}.{table}"


def build_dest_sqls(
    destination: DestinationConnection, column_list: str, wildcard_symbol: str
) -> Union[str, str]:
    """Generates sql `insert` and `truncate` queries.

    Args:
        destination (DestinationConnection): Object with connection
            details as schema and table.
        column_list (str): Columns names to be inserted on destination.
        wildcard_symbol (str): Db symbol for insert statement.
            E.g.: ? for mssql or %s to postgres

    Returns:
        Union[str, str]: `insert` and `truncate` sql queries.
    """


    columns = ", ".join(f'"{col}"' for col in column_list)

    values = ", ".join([wildcard_symbol for i in range(len(column_list))])
    insert = (
        f"INSERT INTO {destination.schema}.{destination.table} ({columns}) "
        f"VALUES ({values})"
    )

    truncate = f"TRUNCATE TABLE {destination.schema}.{destination.table}"

    return insert, truncate


def insert_df_to_db(
    df: pd.DataFrame,
    conn_id: str,
    schema: str,
    table: str,
    reflect_col_table: bool = True,
):
    """Inserts the records from the DataFrame df into the specified
    table. Inserts only the columns that exist in the table.

    TODO: Register operation on `log control`.
    """

    if reflect_col_table:
        # Filter existing table columns
        cols = get_table_cols_name(conn_id=conn_id, schema=schema, table=table)
        cols = [col.lower() for col in cols]
        df.columns = df.columns.str.lower()
        df = df[cols]

    df.to_sql(
        name=table,
        schema=schema,
        con=get_mssql_odbc_engine(conn_id),
        if_exists="append",
        index=False,
    )

def _copy_table_comments(
    source: SourceConnection, destination: DestinationConnection
) -> None:
    """Copy table and column comments/descriptions between databases.

    Args:
        source (SourceConnection): Connection object containing the
            source database information.
        destination (DestinationConnection): Connection object
            containing the destination database information.

    Returns:
        None.
    """

    source_table_comments = TableComments(
        conn_id=source.conn_id, schema=source.schema, table=source.table
    )

    destination_table_comments = TableComments(
        conn_id=destination.conn_id, schema=destination.schema, table=destination.table
    )

    destination_table_comments.put_table_comments(
        table_comments=source_table_comments.table_comments
    )


def save_load_info(
    source: SourceConnection,
    destination: DestinationConnection,
    load_type: str,
    rows_loaded: int,
):
    """Inserts metadata information into a database about a data ingestion
    process, including the origin of the data, the type of ingestion
    (full or incremental), the destination database, the schema where
    the control data will be stored, and the number of rows loaded.

    Args:
        source (SourceConnection): Object with connection info to the
            source database (conn_id, schema, table).
        destination (DestinationConnection): Object with connection info
            to the destination database (conn_id, schema).
        load_type (str): Type of data ingestion, "full" or "incremental".
        rows_loaded (int): Number of rows loaded in the transaction.

    Returns:
        None.
    """

    load_info = LoadInfo(
        source_conn_id=source.conn_id,
        source_schema=source.schema,
        source_table=source.table,
        load_type=load_type,
        dest_conn_id=destination.conn_id,
        log_schema_name=destination.schema,
    )

    load_info.save(rows_loaded)


def get_schema_table_from_query(query: str) -> Union[str, str]:
    """Returns schema and table from a sql query string statement.

    Args:
        query (str): sql query statement.

    Returns:
        schema, table (Union[str, str]): schema and table strings.
    """

    # search pattern "from schema.table" on query
    sintax_from = re.search(
        r"from\s+\"?\'?\[?[\w|\.|\"|\'|\]|\]]*\"?\'?\]?", query, re.IGNORECASE
    ).group()
    # split "from " from "schema.table" and get schema.table[-1]
    db_schema_table = sintax_from.split()[-1]
    # clean `[`, `]`, `"`, `'`
    db_schema_table = re.sub(r"\[|\]|\"|\'", "", db_schema_table)
    # clean "dbo." if exists as dbo.schema.table
    try:
        schema, table = db_schema_table.split(".")[-2:]
    except ValueError:
        schema, table = "multiple", "multiple"

    return schema, table


def copy_db_to_db(
    source: Dict[str, str],
    destination: Dict[str, str],
    columns_to_ignore: list = None,
    destination_truncate: bool = True,
    chunksize: int = 1000,
    copy_table_comments: bool = False,
    load_type: str = "full",
) -> None:
    """Load data from Postgres/MSSQL/MySQL to Postgres/MSSQL using psycopg2
    and pyodbc copying all existing columns and rows in the destination
    table.

    The destination table:
        * can be created if not exists
        * must have matching `ddl` with source table column names
        * can be loaded with provided query on the source table

    Some data types used in the destination table may cause problems
    in copying.
    This list consolidates known cases:
    * **float**: change to **numeric(x,y)** or **decimal(x,y)**
    * **text**: change to **varchar(max)** or **nvarchar**
    * for dates: use **date** for only dates, **datetime** for
    date with time, and **datetime2** for timestamp

    Example:
        copy_db_to_db(
            {"conn_id": "conn_id", "schema": "schema", "table: "table"},
            {"conn_id": "conn_id", "schema": "schema", "table: "table"}
        )

    Args:
        source (Dict[str, str]): A dictionary containing connection
            information for the source database.
            conn_id (str): Airflow connection id.
            schema (str): Source information `schema` name.
            table (str): Source information `table` name.

            source dict expects these keys:
                * conn_id -> required
                * schema and table -> required if `query` not provided.
                * query -> required if `schema` and `table` not provided.

        destination (Dict[str, str]): A dictionary containing connection
            information for the destination database.
            conn_id (str): Airflow connection id.
            schema (str): Destination information `schema` name.
            table (str): Destination information `table` name.

            destination dict expects these keys:
                * conn_id -> required
                * schema -> required
                * table -> required

        columns_to_ignore (list, optional): A list of column names to
            ignore during the copy operation. Defaults to None.
        destination_truncate (bool, optional): If True, the destination
            table will be truncated before copying data. Defaults to True.
        chunksize (int, optional): The number of rows to copy at once.
            Defaults to 1000.
        copy_table_comments (bool, optional): If True, comments on the
            source table will be copied to the destination table.
            Defaults to False.
        load_type (str, optional): The type of load to perform. Can be
            "full" or "incremental". Defaults to "full".

    Return:
        None
    """

    # validate connections
    source = SourceConnection(**source)
    destination = DestinationConnection(**destination)

    # create table if not exists in destination db
    if not source.query:
        create_table_if_not_exists(source, destination)

        if copy_table_comments:
            _copy_table_comments(source, destination)

    # create connections
    with DbConnection(source.conn_id) as source_conn:
        with DbConnection(destination.conn_id) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:
                    # Fast etl
                    if destination.conn_type == "mssql":
                        destination_conn.autocommit = False
                        destination_cur.fast_executemany = True
                        wildcard_symbol = "?"
                    else:
                        wildcard_symbol = "%s"

                    # generate queries
                    col_list = get_table_cols_name(
                        conn_id=destination.conn_id,
                        schema=destination.schema,
                        table=destination.table,
                        columns_to_ignore=columns_to_ignore,
                    )

                    insert, truncate = build_dest_sqls(
                        destination, col_list, wildcard_symbol
                    )
                    if source.query:
                        select_sql = source.query
                        source.schema, source.table = get_schema_table_from_query(
                            source.query
                        )
                    else:
                        select_sql = build_select_sql(
                            schema=source.schema,
                            table=source.table,
                            column_list=col_list,
                        )

                    # remove quotes for mysql compatibility
                    if source.conn_type == "mysql":
                        select_sql = select_sql.replace('"', "")

                    # truncate stage
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        if destination.conn_type == "mssql":
                            destination_cur.commit()

                    # download data
                    start_time = time.perf_counter()
                    source_cur.execute(select_sql)
                    rows = source_cur.fetchmany(chunksize)
                    rows_inserted = 0

                    logging.info(
                        "Loading rows on table [%s].[%s]",
                        destination.schema,
                        destination.table,
                    )
                    while rows:
                        if destination.conn_type == "postgres":
                            psycopg2.extras.execute_batch(destination_cur, insert, rows)
                        else:
                            destination_cur.executemany(insert, rows)
                        rows_inserted += len(rows)
                        rows = source_cur.fetchmany(chunksize)
                        logging.info("%d rows loaded!!", rows_inserted)

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time

                    save_load_info(
                        source=source,
                        destination=destination,
                        load_type=load_type,
                        rows_loaded=rows_inserted,
                    )

                    logging.info("Load time: %f seconds", delta_time)
                    logging.info("Rows insertes: %d", rows_inserted)
                    logging.info("lines by second: %f", rows_inserted / delta_time)


def _table_rows_count(db_hook, table: str, where_condition: str = None):
    """Calculates the number of rows in the table and uses the condition
    (where_condition) if passed as a parameter.
    """

    sql = f"SELECT COUNT(*) FROM {table}"
    sql += f" WHERE {where_condition};" if where_condition is not None else ";"

    return db_hook.get_first(sql)[0]


def _build_filter_condition(
    dest_hook: MsSqlHook,
    table: str,
    date_column: str,
    key_column: str,
    since_datetime: datetime = None,
) -> Tuple[str, str]:
    """Builds the filter (where) by obtaining the max() value from the table,
    distinguishing whether the column is the "date or update datetime"
    (date_column) or another sequential number (key_column). For example,
    id, pk, etc. If the "since_datetime" parameter is provided, it will
    be considered instead of the max() value from the table.

    Example:
        _build_filter_condition(dest_hook=dest_hook,
                        table=table,
                        date_column=date_column,
                        key_column=key_column)

    Args:
        dest_hook (str): destination database connection hook.
        table (str): table to be synchronized.
        date_column (str): name of the column to be used for
            identification of updated records.
        key_column (str): name of the column to be used as a key in the
            step of updating old records that have been updated on
            source.
        since_datetime (datetime): date/time from which the filter will be
            built, instead of using the max() value from the table.

    Returns:
        Tuple[str, str]: Tuple containing the maximum value and the where
            condition of the SQL query.
    """

    if since_datetime:
        max_value = since_datetime
    else:
        if date_column:
            sql = f"SELECT MAX({date_column}) FROM {table}"
        else:
            sql = f"SELECT MAX({key_column}) FROM {table}"

        max_value = dest_hook.get_first(sql)[0]

    if date_column:
        # Checks if the format of the max_value field is date or datetime
        if isinstance(max_value, date):
            max_value = max_value.strftime("%Y-%m-%d")
        elif isinstance(max_value, datetime):
            max_value = max_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        where_condition = f"{date_column} > '{max_value}'"
    else:
        max_value = str(max_value)
        where_condition = f"{key_column} > '{max_value}'"

    return max_value, where_condition


def _build_incremental_sqls(
    dest_table: str, source_table: str, key_column: str, column_list: str
):
    """Builds the SQL queries that perform the updates of the source updated
    records since the last synchronization and the inserts of new records.
    """

    cols = ", ".join(f"{col} = orig.{col}" for col in column_list)
    updates_sql = f"""
            UPDATE {dest_table} SET {cols}
            FROM {source_table} orig
            WHERE orig.{key_column} = {dest_table}.{key_column}
            """
    cols = ", ".join(column_list)
    inserts_sql = f"""INSERT INTO {dest_table} ({cols})
            SELECT {cols}
            FROM {source_table} AS inc
            WHERE NOT EXISTS
            (SELECT 1 FROM {dest_table} AS atual
                WHERE atual.{key_column} = inc.{key_column})
            """
    return updates_sql, inserts_sql


def sync_db_2_db(
    source_conn_id: str,
    destination_conn_id: str,
    table: str,
    date_column: str,
    key_column: str,
    source_schema: str,
    destination_schema: str,
    increment_schema: str,
    select_sql: str = None,
    since_datetime: datetime = None,
    sync_exclusions: bool = False,
    source_exc_schema: str = None,
    source_exc_table: str = None,
    source_exc_column: str = None,
    chunksize: int = 1000,
    copy_table_comments: bool = False,
) -> None:
    """Performs incremental update on a table. The synchronization is
    performed in 3 steps.
        1 - Sends the necessary changes to an intermediate table
        located in the `increment_schema` schema.
        2 - Performs Updates.
        3 - Performs Inserts. Only columns that exists at destination
        table will be synchronized.

    Works with Postgres as source and MsSql as destination. `sync_db_2_db`
    also optionally performs synchronization of deletions.

    Example:
        sync_db_2_db(source_conn_id=SOURCE_CONN_ID,
                     destination_conn_id=DEST_CONN_ID,
                     table=table,
                     date_column=date_column,
                     key_column=key_column,
                     source_schema=SOURCE_SCHEMA,
                     destination_schema=STG_SCHEMA,
                     chunksize=CHUNK_SIZE)

    Args:
        source_conn_id (str): Airflow connection string of the source DB.
        destination_conn_id (str): Airflow connection string of the
            destination DB.
        table (str): Table to be synchronized
        date_column (str): Name of the column to be used for
            identifying updated records in the source.
        key_column (str): Name of the column to be used as a key in
            the update step of old records that have been updated in the source.
        source_schema (str): Schema of the DB in the source.
        destination_schema (str): schema of the DB in the destination.
        increment_schema (str): Schema in the database used for temporary
            tables. If this variable is None, the table will be created
            at the same destiny schema with the suffix '_alteracoes'
        select_sql (str): customized select to use in the load instead of
            replicating the columns of the source table. Should not be used with
            JOINS, only for a single table.
        since_datetime (datetime): date/time from which the increment
            will be performed, overriding the maximum date/time of the destination table.
        sync_exclusions (bool): option to synchronize exclusions.
            Default = False.
        source_exc_schema (str): schema of the table in the source where
            exclusions are registered
        source_exc_table (str): table in the source where exclusions are registered
        source_exc_column (str): column in the table in the source where
            exclusions are registered
        chunksize (int): read block size in the source.
        Default = 1000 rows
        copy_table_comments (bool): flag if includes on the execution the
            copy of table comments/descriptions. Default to False.

    Return:
        None
    """

    def _divide_chunks(l, n):
        """Split list into a new list with n lists"""
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    source_table_name = f"{source_schema}.{table}"
    dest_table_name = f"{destination_schema}.{table}"
    if increment_schema:
        inc_table_name = f"{increment_schema}.{table}"
    else:
        inc_table_name = f"{destination_schema}.{table}_alteracoes"

    source_hook = PostgresHook(postgres_conn_id=source_conn_id, autocommit=True)
    dest_hook, _ = get_hook_and_engine_by_provider(destination_conn_id)

    col_list = get_table_cols_name(destination_conn_id, destination_schema, table)

    dest_rows_count = _table_rows_count(dest_hook, dest_table_name)
    logging.info("Total rows at destination table: %d.", dest_rows_count)
    # If empty table, to avoid error on _build_filter_condition()
    if dest_rows_count == 0:
        raise Exception("Destination table empty. Use full load option.")

    ref_value, where_condition = _build_filter_condition(
        dest_hook, dest_table_name, date_column, key_column, since_datetime
    )
    new_rows_count = _table_rows_count(source_hook, source_table_name, where_condition)
    logging.info("New or modified rows total: %d.", new_rows_count)

    # store updates and inserts
    if not select_sql:
        select_sql = build_select_sql(
            schema=source_schema, table=table, column_list=col_list
        )
    select_diff = f"{select_sql} WHERE {where_condition}"
    logging.info("SQL Query to mirror tables: %s", select_diff)

    copy_db_to_db(
        source={
            "conn_id": source_conn_id,
            "query": select_diff,
            "schema": source_table_name.split(".")[0],
            "table": source_table_name.split(".")[1],
        },
        destination={
            "conn_id": destination_conn_id,
            "schema": inc_table_name.split(".")[0],
            "table": inc_table_name.split(".")[1],
        },
        destination_truncate=True,
        chunksize=chunksize,
        load_type="incremental",
    )

    # rebuild index
    destination_conn_type = get_conn_type(destination_conn_id)
    if destination_conn_type == "mssql":
        sql = f"ALTER INDEX ALL ON {inc_table_name} REBUILD"
    elif destination_conn_type == "postgres":
        sql = f"REINDEX TABLE {inc_table_name}"

    dest_hook.run(sql)

    logging.info("Starting incremental load on table %s.", dest_table_name)
    updates_sql, inserts_sql = _build_incremental_sqls(
        dest_table=f"{dest_table_name}",
        source_table=f"{inc_table_name}",
        key_column=key_column,
        column_list=col_list,
    )

    dest_hook.run(updates_sql)
    dest_hook.run(inserts_sql)

    # if needed to delete rows at destination
    if sync_exclusions:
        source_exc_sql = f"""SELECT {key_column}
                             FROM {source_exc_schema}.{source_exc_table}
                             WHERE {source_exc_column} > '{ref_value}'
                          """
        rows = source_hook.get_records(source_exc_sql)
        ids_to_del = [row[0] for row in rows]

        if ids_to_del:
            ids_to_del_split = _divide_chunks(ids_to_del, 500)
            for chunk in ids_to_del_split:
                ids = ", ".join(str(id) for id in chunk)
                sql = f"""
                    DELETE FROM {dest_table_name}
                    WHERE {key_column} IN ({ids})
                """
                dest_hook.run(sql)

        logging.info(
            "Approximated number of rows deleted: %d", len(ids_to_del)
        )

    # update table descriptions/comments
    if copy_table_comments:
        _copy_table_comments(
            source=SourceConnection(
                conn_id=source_conn_id,
                schema=source_table_name.split(".")[0],
                table=source_table_name.split(".")[1],
            ),
            destination=DestinationConnection(
                conn_id=destination_conn_id,
                schema=dest_table_name.split(".")[0],
                table=dest_table_name.split(".")[1],
            ),
        )
