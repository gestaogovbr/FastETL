# Thanks to Jedi Wash! *.*
"""
Módulo cópia de dados entre Postgres e MsSql e outras coisas
"""

import os
import time
from datetime import datetime, date
import re
import warnings
import urllib
from typing import List, Union, Tuple
import pyodbc
import ctds
import ctds.pool
import logging
import pandas as pd
from pandas.io.sql import DatabaseError
from psycopg2 import OperationalError
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData, inspect
from sqlalchemy.engine import reflection, Engine
from sqlalchemy.sql import sqltypes as sa_types
import sqlalchemy.dialects as sa_dialects
from alembic.migration import MigrationContext
from alembic.operations import Operations


from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.hooks.base import BaseHook
from airflow.hooks.dbapi import DbApiHook

from FastETL.custom_functions.utils.load_info import LoadInfo


class DbConnection:
    """
    Gera as conexões origem e destino dependendo do tipo de provider.
    Providers disponíveis: 'MSSQL', 'PG' e 'MYSQL'
    """

    def __init__(self, conn_id: str, provider: str):
        # Valida providers suportados
        providers = ["MSSQL", "PG", "POSTGRES", "MYSQL"]
        provider = provider.upper()
        assert provider in providers, (
            "Provider não suportado " "(utilize MSSQL, PG ou MYSQL) :P"
        )

        if provider == "MSSQL":
            conn_values = BaseHook.get_connection(conn_id)
            driver = "{ODBC Driver 17 for SQL Server}"
            server = conn_values.host
            port = conn_values.port
            database = conn_values.schema
            user = conn_values.login
            password = conn_values.password
            self.mssql_conn_string = f"""Driver={driver};\
                Server={server}, {port};\
                Database={database};\
                Uid={user};\
                Pwd={password};"""
        elif provider == "PG" or provider == "POSTGRES":
            self.pg_hook = PostgresHook(postgres_conn_id=conn_id)
        elif provider == "MYSQL":
            self.msql_hook = MySqlHook(mysql_conn_id=conn_id)
        self.provider = provider

    def __enter__(self):
        if self.provider == "MSSQL":
            try:
                self.conn = pyodbc.connect(self.mssql_conn_string)
            except:
                raise Exception("MsSql connection failed.")
        elif self.provider == "PG" or self.provider == "POSTGRES":
            try:
                self.conn = self.pg_hook.get_conn()
            except:
                raise Exception("PG connection failed.")
        elif self.provider == "MYSQL":
            try:
                self.conn = self.msql_hook.get_conn()
            except:
                raise Exception("MYSQL connection failed.")
        return self.conn

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def build_select_sql(source_table: str, column_list: str) -> str:
    """
    Monta a string do select da origem
    """

    columns = ", ".join(col for col in column_list)

    return f"SELECT {columns} FROM {source_table}"


def build_dest_sqls(
    destination_table: str, column_list: str, wildcard_symbol: str
) -> Union[str, str, str]:
    """
    Monta a string do insert do destino
    Monta a string de truncate do destino
    """

    columns = ", ".join(col for col in column_list)

    values = ", ".join([wildcard_symbol for i in range(len(column_list))])
    insert = f"INSERT INTO {destination_table} ({columns}) " f"VALUES ({values})"

    truncate = f"TRUNCATE TABLE {destination_table}"

    return insert, truncate


def get_cols_name(
    cur, destination_provider: str, destination_table: str, columns_to_ignore: list = []
) -> List[str]:
    """
    Obtem as colunas da tabela de destino
    """

    if destination_provider.upper() == "MSSQL":
        colnames = []
        cur.execute(f"SELECT * FROM {destination_table} WHERE 1 = 2")
        for col in cur.columns(
            schema=destination_table.split(".")[0],
            table=destination_table.split(".")[1],
        ):
            colnames.append(col.column_name)
    elif destination_provider.upper() == "PG":
        cur.execute(f"SELECT * FROM {destination_table} WHERE 1 = 2")
        colnames = [desc[0] for desc in cur.description]

    colnames = [n for n in colnames if n not in columns_to_ignore]

    return [f'"{n}"' for n in colnames]


def get_table_cols_name(conn_id: str, schema: str, table: str):
    """
    Obtem a lista de colunas de uma tabela.
    """
    conn_values = BaseHook.get_connection(conn_id)

    if conn_values.conn_type == "mssql":
        db_hook = MsSqlHook(mssql_conn_id=conn_id)
    elif conn_values.conn_type == "postgres":
        db_hook = PostgresHook(postgres_conn_id=conn_id)
    else:
        raise Exception("Conn_type not implemented.")

    with db_hook.get_conn() as db_conn:
        with db_conn.cursor() as db_cur:
            db_cur.execute(f"SELECT * FROM {schema}.{table} WHERE 1=2")
            column_names = [tup[0] for tup in db_cur.description]

    return column_names


def get_mssql_odbc_conn_str(conn_id: str):
    """
    Cria uma string de conexão com banco SQL Server usando driver pyodbc.
    """
    conn_values = BaseHook.get_connection(conn_id)
    driver = "{ODBC Driver 17 for SQL Server}"
    server = conn_values.host
    port = conn_values.port
    database = conn_values.schema
    user = conn_values.login
    password = conn_values.password

    mssql_conn = f"""Driver={driver};Server={server}, {port}; \
                    Database={database};Uid={user};Pwd={password};"""

    quoted_conn_str = urllib.parse.quote_plus(mssql_conn)

    return f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}"


def get_mssql_odbc_engine(conn_id: str):
    """
    Cria uma engine de conexão com banco SQL Server usando driver pyodbc.
    """
    return create_engine(get_mssql_odbc_conn_str(conn_id))


def insert_df_to_db(
    df: pd.DataFrame,
    conn_id: str,
    schema: str,
    table: str,
    reflect_col_table: bool = True,
):
    """
    Insere os registros do DataFrame df na tabela especificada. Insere
    apenas as colunas que existem na tabela.

    TODO: Implementar aqui o registro no LOG CONTROLE
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


def validate_db_string(
    source_table: str, destination_table: str, select_sql: str
) -> None:
    """
    Valida se string do banco está no formato schema.table e se tabelas de
    origem e destino possuem o mesmo nome. Se possui select_sql não valida a
    source_table
    """

    assert (
        destination_table.count(".") == 1
    ), "Estrutura tabela destino deve ser str: schema.table"

    if not select_sql:
        assert (
            source_table.count(".") == 1
        ), "Estrutura tabela origem deve ser str: schema.table"

        if source_table.split(".")[1] != destination_table.split(".")[1]:
            warnings.warn("Tabelas de origem e destino com nomes diferentes")


def compare_source_dest_rows(
    source_cur, destination_cur, source_table: str, destination_table: str
) -> None:
    """
    Compara quantidade de linhas na tabela origem e destino após o ETL.
    Caso diferente, imprime warning. Quando a tabela de origem está
    diretamente ligada ao sistema transacional justifica-se a diferença.
    """

    source_cur.execute(f"SELECT COUNT(*) FROM {source_table}")
    destination_cur.execute(f"SELECT COUNT(*) FROM {destination_table}")

    source_row_count = source_cur.fetchone()[0]
    destination_row_count = destination_cur.fetchone()[0]

    if source_row_count != destination_row_count:
        warnings.warn(
            "Quantidade de linhas diferentes na origem e destino. "
            f"Origem: {source_row_count} linhas. "
            f"Destino: {destination_row_count} linhas"
        )


def get_hook_and_engine_by_provider(
    provider: str, conn_id: str
) -> Tuple[DbApiHook, Engine]:
    provider = provider.upper()

    if provider == "MSSQL":
        hook = MsSqlHook(conn_id)
        engine = get_mssql_odbc_engine(conn_id)
    elif provider == "PG" or provider == "POSTGRES":
        hook = PostgresHook(conn_id)
        engine = hook.get_sqlalchemy_engine()
    elif provider == "MYSQL":
        hook = MySqlHook(conn_id)
        engine = hook.get_sqlalchemy_engine()

    return hook, engine


def _convert_column(old_col: Column, db_provider: str) -> Column:
    type_mapping = {
        "NUMERIC": sa_types.Numeric(38, 13),
        "BIT": sa_types.Boolean(),
    }
    if db_provider.upper() == "MSSQL":
        type_mapping["DATETIME"] = sa_dialects.mssql.DATETIME2()

    return Column(
        old_col["name"],
        type_mapping.get(
            str(old_col["type"]._type_affinity()), old_col["type"]._type_affinity()
        ),
    )


def create_table_if_not_exist(
    source_table: str,
    source_conn_id: str,
    source_provider: str,
    destination_table: str,
    destination_conn_id: str,
    destination_provider: str,
    copy_table_comments: bool,
) -> None:
    ERROR_TABLE_DOES_NOT_EXIST = {
        "MSSQL": "Invalid object name",
        "PG": "does not exist",
    }
    _, source_eng = get_hook_and_engine_by_provider(source_provider, source_conn_id)
    destination_hook, destination_eng = get_hook_and_engine_by_provider(
        destination_provider, destination_conn_id
    )
    try:
        destination_hook.get_pandas_df(f"select * from {destination_table} where 1=2")
    except (DatabaseError, OperationalError, NoSuchModuleError) as db_error:
        if not ERROR_TABLE_DOES_NOT_EXIST[destination_provider] in str(db_error):
            raise db_error
        # Table does not exist so we create it
        source_eng.echo = True
        try:
            insp = reflection.Inspector.from_engine(source_eng)
        except AssertionError as e:
            logging.error(
                "Não é possível criar tabela automaticamente "
                "a partir deste banco de dados. Crie a tabela "
                "manualmente para executar a cópia dos dados. "
            )
            raise e

        s_schema, s_table = source_table.split(".")

        generic_columns = insp.get_columns(s_table, s_schema)
        dest_columns = [
            _convert_column(c, destination_provider) for c in generic_columns
        ]

        destination_meta = MetaData(bind=destination_eng)
        d_schema, d_table = destination_table.split(".")
        Table(d_table, destination_meta, *dest_columns, schema=d_schema)

        destination_meta.create_all(destination_eng)

    if copy_table_comments:
        _copy_table_comments(
            source_conn_id=source_conn_id,
            source_table=source_table,
            destination_conn_id=destination_conn_id,
            destination_table=destination_table,
        )


def _copy_table_comments(
    source_conn_id: str,
    source_table: str,
    destination_conn_id: str,
    destination_table: str,
) -> None:
    """Copy table and colunms comments/descriptions between databases.

    Args:
        source_conn_id (str): Airflow connection id
        source_table (str): Table str at format schema.table
        destination_conn_id (str): Airflow connection id
        destination_table (str): Table str at format schema.table

    Returns:
        None
    """

    source_table_comments = TableComments(
        conn_id=source_conn_id, schema_table=source_table
    )

    destination_table_comments = TableComments(
        conn_id=destination_conn_id, schema_table=destination_table
    )

    destination_table_comments.put_table_comments(
        table_comments=source_table_comments.table_comments
    )


class TableComments:
    """
    Retrieve and save table comments/descriptions (including columns) from one
    database to another. Accepts on the origin teiid, mssql and postgres.
    And accepts to the destination mssql and postgres.
    """

    def __init__(self, conn_id: str, schema_table: str):
        """Initialize TableComments class variables.

        Args:
            conn_id (str): Airflow connection id
            schema_table (str): Table str at format schema.table
        """

        self.conn_id = conn_id
        self.schema, self.table = schema_table.split(".")
        conn_values = BaseHook.get_connection(conn_id)
        self.conn_type = conn_values.conn_type
        self.conn_database = conn_values.schema
        self.table_comments_init = pd.DataFrame(
            columns=["database_level", "name", "comment"]
        )
        self.cols_names = None
        self.mssql_hook = None
        self._table_comments = None

    @property
    def table_comments(self):
        if getattr(self, "_table_comments", None) is None:
            self._table_comments = self.get_table_comments_df()

        return self._table_comments

    @table_comments.setter
    def table_comments(self, df):
        self._table_comments = df

    def _get_mssql_table_comments(self) -> pd.DataFrame:
        """Get from mssql database comments/descriptions of provided
        table and its columns. Uses MSSql stored procedure
        fn_listextendedproperty.

        Returns:
            pd.Dataframe: Concat of table and its columns comments/descriptions

        Reference:
            https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-listextendedproperty-transact-sql?view=sql-server-ver16
        """

        mssql_hook = MsSqlHook(self.conn_id)
        table_comments = self.table_comments_init.copy()

        for database_level, query_param in {
            "table": "default",
            "column": "'COLUMN'",
        }.items():
            rows_df = mssql_hook.get_pandas_df(
                f"""
                    SELECT objname,
                        value
                    FROM fn_listextendedproperty
                        ('MS_DESCRIPTION',
                        'schema', '{self.schema}',
                        'table', '{self.table}',
                        {query_param}, default);
                """
            )
            rows_df.rename(
                columns={"objname": "name", "value": "comment"}, inplace=True
            )
            rows_df["comment"] = rows_df["comment"].str.decode("utf-8")
            rows_df["database_level"] = database_level
            table_comments = table_comments.append(rows_df, ignore_index=True)

        return table_comments

    def _get_pg_table_comments(self) -> pd.DataFrame:
        """Get from postgres database comments/descriptions of provided
        table and its columns. Uses SQLAlchemy library.

        Returns:
            pd.DataFrame: Concat of table and its columns comments/descriptions

        Reference:
            https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_table_comment
            https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_columns
        """

        _, engine = get_hook_and_engine_by_provider(
            provider="postgres", conn_id=self.conn_id
        )
        inspector = inspect(engine)

        table_info = inspector.get_table_comment(
            table_name=self.table, schema=self.schema
        )
        table_comments = self.table_comments_init.copy()
        table_comments = table_comments.append(
            {
                "database_level": "table",
                "name": self.table,
                "comment": table_info["text"],
            },
            ignore_index=True,
        )

        columns_info = inspector.get_columns(table_name=self.table, schema=self.schema)
        for row in columns_info:
            table_comments = table_comments.append(
                {
                    "database_level": "column",
                    "name": row["name"],
                    "comment": row["comment"],
                },
                ignore_index=True,
            )

        return table_comments

    def _get_teiid_table_comments(self) -> pd.DataFrame:
        """Get from teiid database connection comments/descriptions
        of provided table and its columns. Uses database SYS.Tables
        and SYS.Columns information tables.

        Returns:
            pd.DataFrame: concat of table and its columns comments/descriptions
        """

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        queries = {
            "table": f"""
                SELECT Name,
                    Description
                FROM SYS.Tables
                WHERE VDBName = '{self.conn_database}'
                    and SchemaName = '{self.schema}'
                    and Name = '{self.table}'
                """,
            "column": f"""
                    SELECT Name,
                        Description
                    FROM SYS.Columns
                    WHERE VDBName = '{self.conn_database}'
                        and SchemaName = '{self.schema}'
                        and TableName = '{self.table}'
                """,
        }

        table_comments = self.table_comments_init.copy()
        for database_level, query in queries.items():
            rows_df = pg_hook.get_pandas_df(query)
            rows_df.rename(
                columns={"Name": "name", "Description": "comment"}, inplace=True
            )
            rows_df["database_level"] = database_level
            table_comments = table_comments.append(rows_df, ignore_index=True)

        return table_comments

    def _get_mssql_stored_procedure_str(
        self, database_level: str, col_name: str = None
    ) -> str:
        """Check database table and columns descriptions and returns
        mssql stored procedure command name depending if the column/table
        has already a description.

        Args:
            database_level (str): If `table` or `colunm`
            col_name (str, optional): When `column` database_level, the
                name of the column. Defaults to None.

        Raises:
            Exception: when the database level is neither
                'table' or 'column'.

        Returns:
            str: `updateextendedproperty` or `addextendedproperty`
                depending if the description already exists on the
                table/column.

        Reference:
            https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-addextendedproperty-transact-sql?view=sql-server-ver16
            https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-updateextendedproperty-transact-sql?view=sql-server-ver16
            https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-listextendedproperty-transact-sql?view=sql-server-ver16
        """

        if database_level == "table":
            query = f"""
                SELECT value
                FROM fn_listextendedproperty (
                'MS_Description',
                'schema',
                '{self.schema}',
                'table',
                '{self.table}',
                default,
                default);
            """

        elif database_level == "column" and col_name:
            query = f"""
                SELECT value
                FROM fn_listextendedproperty (
                'MS_Description',
                'schema',
                '{self.schema}',
                'table',
                '{self.table}',
                'column',
                '{col_name}');
            """

        else:
            raise ValueError(
                """MSSQL database level type not implemented or column
                name not provided. PR for the best."""
            )

        stored_procedure = (
            "updateextendedproperty"
            if self.mssql_hook.get_first(query)
            else "addextendedproperty"
        )

        return stored_procedure

    def _get_comment_value(
        self, database_level: str, col_name: str = None
    ) -> pd.Series:
        """Filter pandas dataframe to get the value of the comment
        column based on table or column values.

        Args:
            database_level (str): If `table` or `column`
            col_name (str, optional): Name of the column. Defaults to None.

        Raises:
            Exception: When database_level arg not in ['table', 'column']

        Returns:
            pd.Series: Filtered pandas series with matched row
        """

        if database_level == "table":
            comment = self.table_comments.loc[
                self.table_comments.database_level == database_level, "comment"
            ]

        elif database_level == "column" and col_name:
            comment = self.table_comments.loc[
                (self.table_comments.database_level == database_level)
                & (self.table_comments.name.str.match(col_name, case=False)),
                "comment",
            ]

        else:
            raise ValueError(
                """
                Database_level only accepts `table` or `column`.
                If column, must provide column name.
                """
            )

        # Convert None values to empty pd.Series
        if not comment.empty and comment.values[0] is None:
            comment = pd.Series()

        return comment

    def _put_mssql_table_comments(self) -> None:
        """Write comments/descriptions on database table and its columns
        for mssql.

        Returns:
            None.

        Reference:
            https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-addextendedproperty-transact-sql?view=sql-server-ver16
            https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-updateextendedproperty-transact-sql?view=sql-server-ver16
        """

        # Part 1 - check if table description exists

        stored_procedure_str = self._get_mssql_stored_procedure_str(
            database_level="table"
        )

        # Part 2 - add or update table description

        comment = self._get_comment_value(database_level="table")

        if not comment.empty:
            self.mssql_hook.run(
                f"""
                EXEC sys.sp_{stored_procedure_str}
                @name='MS_Description',
                @value='{comment.values[0]}',
                @level0type='schema',
                @level0name='{self.schema}',
                @level1type='table',
                @level1name='{self.table}'
                """
            )

        for col_name in self.cols_names:

            # Part 3 - check if columns description exists

            stored_procedure_str = self._get_mssql_stored_procedure_str(
                database_level="column", col_name=col_name
            )

            # Part 4 - add or update columns description

            comment = self._get_comment_value(
                database_level="column", col_name=col_name
            )

            if not comment.empty:
                self.mssql_hook.run(
                    f"""
                    EXEC sys.sp_{stored_procedure_str}
                    @name='MS_Description',
                    @value='{comment.values[0]}',
                    @level0type='schema',
                    @level0name='{self.schema}',
                    @level1type='table',
                    @level1name='{self.table}',
                    @level2type='column',
                    @level2name='{col_name}'
                    """
                )

    def _put_pg_table_comments(self) -> None:
        """Write comments/descriptions on database table and its columns
        for postgres using SQLAlchemy and Alembic.

        Returns:
            None.

        References:
            https://alembic.sqlalchemy.org/en/latest/ops.html
        """

        _, engine = get_hook_and_engine_by_provider(
            provider="postgres", conn_id=self.conn_id
        )
        conn = engine.connect()
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        # Part 1 - write table comment

        comment = self._get_comment_value(database_level="table")

        if not comment.empty:
            op.create_table_comment(
                table_name=self.table, schema=self.schema, comment=comment.values[0]
            )

        # Part 2 - write columns comments

        for col_name in self.cols_names:

            comment = self._get_comment_value(
                database_level="column", col_name=col_name
            )

            if not comment.empty:
                op.alter_column(
                    table_name=self.table,
                    column_name=col_name,
                    schema=self.schema,
                    comment=comment.values[0],
                )

    def get_table_comments_df(self):
        """Get comments/descriptions of provided table and its columns
        by the type/provider of the database. Implemented for:
            - Postgres
            - teiid
            - MS Sql

        Raises:
            Exception: when the provided database is not in postgres,
                mssql or teiid scope

        Returns:
            pd.DataFrame: concat of table and its columns comments/descriptions
        """

        if self.conn_type == "mssql":
            table_comments = self._get_mssql_table_comments()

        elif self.conn_type == "postgres":
            # Postgres Connection
            try:
                table_comments = self._get_pg_table_comments()

            # teiid driver
            except AssertionError:
                table_comments = self._get_teiid_table_comments()
        else:
            raise NotImplementedError(
                "Database connection type not implemented. PR for the best."
            )

        return table_comments

    def put_table_comments(self, table_comments):
        """Write comments/descriptions on table and its columns
        by the type/provider of the database. Implemented for:
            - Postgres
            - MS Sql

        Args:
            table_comments (pd.DataFrame): Pandas dataframe with
                table and columns comments/descriptions

        Raises:
            Exception: When the provided database is not in postgres or mssql

        Returns:
            None
        """

        self._table_comments = table_comments
        self.cols_names = get_table_cols_name(
            conn_id=self.conn_id, schema=self.schema, table=self.table
        )

        if self.conn_type == "mssql":
            self.mssql_hook = MsSqlHook(self.conn_id)
            self._put_mssql_table_comments()

        elif self.conn_type == "postgres":
            self._put_pg_table_comments()

        else:
            raise NotImplementedError(
                "Database connection type not implemented. PR for the best."
            )

    def save(self):
        """Save Dataframe of table comments on the database."""

        self.put_table_comments(self.table_comments)


def save_load_info(
    source_conn_id: str,
    source_schema_table: str,
    load_type: str,
    dest_conn_id: str,
    log_schema_name: str,
    rows_loaded: int,
):
    load_info = LoadInfo(
        source_conn_id, source_schema_table, load_type, dest_conn_id, log_schema_name
    )

    load_info.save(rows_loaded)


def get_schema_table_from_query(query: str) -> str:
    """Returns schema.table from a query statement.

    Args:
        query (str): sql query statement.

    Returns:
        schema_table (str): string in format `schema.table`
    """

    # search pattern "from schema.table" on query
    sintax_from = re.search(
        r"from\s+\"?\'?\[?[\w|\.|\"|\'|\]|\]]*\"?\'?\]?", query, re.IGNORECASE
    ).group()
    # split "from " from "schema.table" and get schema.table [-1]
    db_schema_table = sintax_from.split()[-1]
    # clean [, ], ", '
    db_schema_table = re.sub(r"\[|\]|\"|\'", "", db_schema_table)
    # clean "dbo." if exists on dbo.schema.table
    schema_table = ".".join(db_schema_table.split(".")[-2:])

    return schema_table


def copy_db_to_db(
    destination_table: str,
    source_conn_id: str,
    source_provider: str,
    destination_conn_id: str,
    destination_provider: str,
    source_table: str = None,
    select_sql: str = None,
    columns_to_ignore: list = [],
    destination_truncate: bool = True,
    chunksize: int = 1000,
    copy_table_comments: bool = False,
    load_type: str = "full",
) -> None:
    """
    Carrega dado do Postgres/MSSQL/MySQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas e linhas já existentes na tabela de destino.
    Tabela de destino deve ter a mesma estrutura e nome de tabela e colunas
    que a tabela de origem, ou passar um select_sql que tenha as colunas de
    destino.

    Alguns tipos de dados utilizados na tabela destino podem gerar
    problemas na cópia. Esta lista consolida os casos conhecidos:
    * **float**: mude para **numeric(x,y)** ou **decimal(x,y)**
    * **text**: mude para **varchar(max)** ou **nvarchar**
    * para datas: utilize **date** para apenas datas, **datetime** para
    data com hora, e **datetime2** para timestamp

    Exemplo:
        copy_db_to_db('Siorg_VBL.contato_email',
                      'SIORG_ESTATISTICAS.contato_email',
                      'POSTG_CONN_ID',
                      'PG',
                      'MSSQL_CONN_ID',
                      'MSSQL')

    Args:
        destination_table (str): tabela de destino no formato schema.table
        source_conn_id (str): connection origem do Airflow
        source_provider (str): provider do banco origem (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        source_table (str): tabela de origem no formato schema.table
        select_sql (str): query sql para consulta na origem. Se utilizado o
            source_table será ignorado
        columns_to_ignore (list): list of columns to be ignored in the copy
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
        chunksize (int): tamanho do bloco de leitura na origem.
            Default = 1000 linhas
        copy_table_comments (bool): flag if includes on the execution the
            copy of table comments/descriptions. Default to False.
        load_type (str): if "full" or "incremental". Default to "full"

    Return:
        None

    Todo:
        * Transformar em Classe ou em Airflow Operator
        * Criar tabela no destino quando não existente
        * Alterar conexão do Postgres para pyodbc
        * Possibilitar inserir data da carga na tabela de destino
        * Criar testes
    """

    # validate db string
    validate_db_string(source_table, destination_table, select_sql)

    # create table if not exists in destination db
    create_table_if_not_exist(
        source_table,
        source_conn_id,
        source_provider,
        destination_table,
        destination_conn_id,
        destination_provider,
        copy_table_comments,
    )

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:
                    # Fast etl
                    if destination_provider == "MSSQL":
                        destination_conn.autocommit = False
                        destination_cur.fast_executemany = True
                        wildcard_symbol = "?"
                    else:
                        wildcard_symbol = "%s"

                    # gera queries
                    col_list = get_cols_name(
                        destination_cur,
                        destination_provider,
                        destination_table,
                        columns_to_ignore,
                    )

                    insert, truncate = build_dest_sqls(
                        destination_table, col_list, wildcard_symbol
                    )
                    if not select_sql:
                        select_sql = build_select_sql(source_table, col_list)

                    # Remove as aspas na query para compatibilidade com o MYSQL
                    if source_provider == "MYSQL":
                        select_sql = select_sql.replace('"', "")

                    # truncate stg
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        if destination_provider == "MSSQL":
                            destination_cur.commit()

                    # download data
                    start_time = time.perf_counter()
                    source_cur.execute(select_sql)
                    rows = source_cur.fetchmany(chunksize)
                    rows_inserted = 0

                    logging.info("Inserindo linhas na tabela [%s].", destination_table)
                    while rows:
                        destination_cur.executemany(insert, rows)
                        rows_inserted += len(rows)
                        rows = source_cur.fetchmany(chunksize)
                        logging.info("%d linhas inseridas!!", rows_inserted)

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time

                    # validate total lines downloaded
                    # compare_source_dest_rows(source_cur,
                    #                           destination_cur,
                    #                           source_table,
                    #                           destination_table)

                    if select_sql:
                        source_table = get_schema_table_from_query(select_sql)

                    save_load_info(
                        source_conn_id=source_conn_id,
                        source_schema_table=source_table,
                        load_type=load_type,
                        dest_conn_id=destination_conn_id,
                        log_schema_name=destination_table.split(".")[0],
                        rows_loaded=rows_inserted,
                    )

                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)


def _table_rows_count(db_hook, table: str, where_condition: str = None):
    """Calcula a quantidade de linhas na tabela (table) e utiliza a
    condição (where_condition) caso seja passada como parâmetro.
    """
    sql = f"SELECT COUNT(*) FROM {table}"
    sql += f" WHERE {where_condition};" if where_condition is not None else ";"
    return db_hook.get_first(sql)[0]


# TODO: Propor ao Wash de passarmos a definir explicitamente a coluna
# 'dataalteracao' na variável airflow de configurações sempre que for o
# caso, e assim pararmos de checar se a coluna 'dataalteracao' está na
# lista de colunas. Consultá-lo sobre drawbacks.


def _table_column_max_string(db_hook: MsSqlHook, table: str, column: str):
    """Calcula o valor máximo da coluna (column) na tabela (table). Se
    a coluna for 'dataalteracao' a string retornada é formatada.
    """
    sql = f"SELECT MAX({column}) FROM {table};"
    max_value = db_hook.get_first(sql)[0]
    # TODO: Descobrir se é data pelo tipo do BD
    if column == "dataalteracao":
        return max_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    else:
        return str(max_value)


def _build_increm_filter(
    col_list: list, dest_hook: MsSqlHook, table: str, key_column: str
) -> str:
    """Constrói a condição where (where_condition) a ser utilizada para
    calcular e identificar as linhas da tabela no BD origem (Quartzo/Serpro)
    que devem ser sincronizadas com aquela tabela no BD destino. Se a
    tabela não possuir a coluna 'dataalteracao' será utilizada a coluna
    (key_column).
    """
    col_list = [col.lower() for col in col_list]
    if "dataalteracao" in col_list:
        key = "dataalteracao"
    else:
        key = key_column
    max_value = _table_column_max_string(dest_hook, table, key)
    where_condition = f"{key} > '{max_value}'"

    return where_condition


def _build_filter_condition(
    dest_hook: MsSqlHook,
    table: str,
    date_column: str,
    key_column: str,
    since_datetime: datetime = None,
) -> Tuple[str, str]:
    """Monta o filtro (where) obtenção o valor max() da tabela,
    distinguindo se a coluna é a "data ou data/hora de atualização"
    (date_column) ou outro número sequencial (key_column), por exemplo
    id, pk, etc. Se o parâmetro "since_datetime" for recebido, será
    considerado em vez do valor max() da tabela.

    Exemplo:
        _build_filter_condition(dest_hook: hook,
                        table=table,
                        date_column=date_column,
                        key_column=key_column)

    Args:
        dest_hook (str): hook de conexão do DB de destino
        table (str): tabela a ser sincronizada
        date_column (str): nome da coluna a ser utilizado para
            identificação dos registros atualizados.
        key_column (str): nome da coluna a ser utilizado como chave na
            etapa de atualização dos registros antigos que sofreram
            atualizações na origem.
        since_datetime (datetime): data/hora a partir do qual o filtro será
            montado, em vez de usar o max() da tabela.

        Returns:
                Tuple[str, str]: Tupla contendo o valor máximo e a condição
                        where da query sql.

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
        # Verifica se o formato do campo max_value é date ou datetime
        if type(max_value) == date:
            max_value = max_value.strftime("%Y-%m-%d")
        elif type(max_value) == datetime:
            max_value = max_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        where_condition = f"{date_column} > '{max_value}'"
    else:
        max_value = str(max_value)
        where_condition = f"{key_column} > '{max_value}'"

    return max_value, where_condition


def _build_incremental_sqls(
    dest_table: str, source_table: str, key_column: str, column_list: str
):
    """Constrói as queries SQLs que realizam os Updates dos registros
    atualizados desde a última sincronização e os Inserts das novas
    linhas.
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
    """Realiza a atualização incremental de uma tabela. A sincronização
    é realizada em 3 etapas. 1-Envia as alterações necessárias para uma
    tabela intermediária localizada no esquema `increment_schema`.
    2-Realiza os Updates. 3-Realiza os Inserts. Apenas as colunas que
    existam na tabela no BD destino serão sincronizadas. Funciona com
    Postgres na origem e MsSql no destino. O algoritmo também realiza,
    opcionalmente, sincronização de exclusões.

    Exemplo:
        sync_db_2_db(source_conn_id=SOURCE_CONN_ID,
                     destination_conn_id=DEST_CONN_ID,
                     table=table,
                     date_column=date_column,
                     key_column=key_column,
                     source_schema=SOURCE_SCHEMA,
                     destination_schema=STG_SCHEMA,
                     chunksize=CHUNK_SIZE)

    Args:
        source_conn_id (str): string de conexão airflow do DB origem
        destination_conn_id (str): string de conexão airflow do DB destino
        table (str): tabela a ser sincronizada
        date_column (str): nome da coluna a ser utilizado para
            identificação dos registros atualizados na origem.
        key_column (str): nome da coluna a ser utilizado como chave na
            etapa de atualização dos registros antigos que sofreram
            atualizações na origem.
        source_eschema (str): esquema do BD na origem
        destination_schema (str): esquema do BD no destino
        increment_schema (str): Esquema no banco utilizado para tabelas
            temporárias. Caso esta variável seja None, esta tabela será
            criada no mesmo schema com sufixo '_alteracoes'
        select_sql (str): select customizado para utilizar na carga ao invés
            de replicar as colunas da tabela origem. Não deve ser utilizado com
            JOINS, apenas para uma única tabela.
        since_datetime (datetime): data/hora a partir da qual o incremento
            será realizado, sobrepondo-se à data/hora máxima da tabela destino
        sync_exclusions (bool): opção de sincronizar exclusões.
            Default = False.
        source_exc_schema (str): esquema da tabela na origem onde estão
            registradas exclusões
        source_exc_table (str): tabela na origem onde estão registradas
            exclusões
        source_exc_column (str): coluna na tabela na origem onde estão
            registradas exclusões
        chunksize (int): tamanho do bloco de leitura na origem.
        Default = 1000 linhas
        copy_table_comments (bool): flag if includes on the execution the
            copy of table comments/descriptions. Default to False.

    Return:
        None

    Todo:
        * Automatizar a criação da tabela gêmea e remoção ao final
        * Transformar em Airflow Operator
        * Possibilitar ler de MsSql e escrever em Postgres
        * Possibilitar inserir data da carga na tabela de destino
        * Criar testes
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

    conn_values = BaseHook.get_connection(destination_conn_id)
    if conn_values.conn_type == "mssql":
        dest_hook = MsSqlHook(mssql_conn_id=destination_conn_id)
        destination_provider = "MSSQL"
    elif conn_values.conn_type == "postgres":
        dest_hook = PostgresHook(postgres_conn_id=destination_conn_id)
        destination_provider = "PG"

    col_list = get_table_cols_name(destination_conn_id, destination_schema, table)

    dest_rows_count = _table_rows_count(dest_hook, dest_table_name)
    logging.info("Total de linhas atualmente na tabela destino: %d.", dest_rows_count)
    # If de tabela vazia separado para evitar erro na _build_filter_condition()
    if dest_rows_count == 0:
        raise Exception("Tabela destino vazia! Utilize carga full!")

    ref_value, where_condition = _build_filter_condition(
        dest_hook, dest_table_name, date_column, key_column, since_datetime
    )
    new_rows_count = _table_rows_count(source_hook, source_table_name, where_condition)
    logging.info("Total de linhas novas ou modificadas: %d.", new_rows_count)

    # Guarda as alterações e inclusões necessárias
    if not select_sql:
        select_sql = build_select_sql(f"{source_table_name}", col_list)
    select_diff = f"{select_sql} WHERE {where_condition}"
    logging.info("SELECT para espelhamento: %s", select_diff)

    copy_db_to_db(
        destination_table=f"{inc_table_name}",
        source_conn_id=source_conn_id,
        source_provider="PG",
        destination_conn_id=destination_conn_id,
        destination_provider=destination_provider,
        source_table=source_table_name,
        select_sql=select_diff,
        destination_truncate=True,
        chunksize=chunksize,
        load_type="incremental",
    )

    # Reconstrói índices
    if conn_values.conn_type == "mssql":
        sql = f"ALTER INDEX ALL ON {inc_table_name} REBUILD"
    elif conn_values.conn_type == "postgres":
        sql = f"REINDEX TABLE {inc_table_name}"

    dest_hook.run(sql)

    logging.info("Iniciando carga incremental na tabela %s.", dest_table_name)
    updates_sql, inserts_sql = _build_incremental_sqls(
        dest_table=f"{dest_table_name}",
        source_table=f"{inc_table_name}",
        key_column=key_column,
        column_list=col_list,
    )
    # Realiza updates
    dest_hook.run(updates_sql)
    # Realiza inserts de novas linhas
    dest_hook.run(inserts_sql)

    # Se precisar aplicar as exclusões da origem no destino:
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
            "Quantidade de linhas possivelmente excluídas: %d", len(ids_to_del)
        )

    # atualiza comentários da tabela
    if copy_table_comments:
        _copy_table_comments(
            source_conn_id,
            source_table_name,
            destination_conn_id,
            dest_table_name,
        )


def write_ctds(table, rows, conn_id):
    """
    Escreve em banco MsSql Server utilizando biblioteca ctds com driver
    FreeTds.
    """

    conn_values = BaseHook.get_connection(conn_id)

    dbconfig = {
        "server": conn_values.host,
        "port": int(conn_values.port),
        "database": conn_values.schema,
        "autocommit": True,
        "enable_bcp": True,
        "ntlmv2": True,
        "user": conn_values.login,
        "password": conn_values.password,
        "timeout": 300,
    }

    pool = ctds.pool.ConnectionPool(ctds, dbconfig)

    with pool.connection() as conn:
        itime = time.perf_counter()
        linhas = conn.bulk_insert(
            table=table,
            rows=rows,
            # batch_size=300000,
        )
        ftime = time.perf_counter()
        logging.info("%d linhas inseridas em %f segundos.", linhas, ftime - itime)


def copy_by_key_interval(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 10000,
    destination_truncate: bool = True,
):
    """
    Carrega dado do Postgres/MSSQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas já existentes na tabela de destino usando
    intervalos de valores da chave da tabela na consulta à origem.
    Tabela de destino deve ter as mesmas colunas ou subconjunto das colunas
    da tabela de origem, e com data types iguais ou compatíveis.

    Exemplo:
        copy_by_key_interval(
                            source_provider='PG',
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_provider='MSSQL',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            key_column='id',
                            key_start=2565,
                            key_interval=1000,
                            destination_truncate=False)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a cópia é iniciada
        key_interval (int): intervalo de id's para ler da origem a cada vez
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True

    Return:
        Tupla (status: bool, next_key: int):
            status: True (sucesso) ou False (falha)
            next_key: id da próxima chave a carregar, quando status = False
                      None: quando status = True, ou outras situações

    TODO:
        * try/except nas aberturas das conexões; se erro: return False, key_start
        * comparar performance do prepared statement no source: psycopg2 x pyodbc
    """

    # validate db string
    validate_db_string(source_table, destination_table, None)

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:

                    # Fast etl
                    if destination_provider == "MSSQL":
                        destination_conn.autocommit = False
                        destination_cur.fast_executemany = True
                        wildcard_symbol = "?"
                    else:
                        wildcard_symbol = "%s"

                    # gera queries
                    col_list = get_cols_name(
                        destination_cur, destination_provider, destination_table
                    )
                    insert, truncate = build_dest_sqls(
                        destination_table, col_list, wildcard_symbol
                    )
                    select_sql = build_select_sql(source_table, col_list)
                    # pyodbc: select_sql = f"{select_sql} WHERE {key_column} BETWEEN ? AND ?"
                    select_sql = f"{select_sql} WHERE {key_column} BETWEEN %s AND %s"

                    # truncate stg
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        destination_cur.commit()

                    # copy by key interval
                    start_time = time.perf_counter()
                    key_begin = key_start
                    key_end = key_begin + key_interval - 1
                    rows_inserted = 0

                    # Tenta LER na origem
                    try:
                        # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchall()
                        source_cur.execute(select_sql, (key_begin, key_end))
                        rows = source_cur.fetchall()  # psycopg2
                    except Exception as e:
                        logging.info(
                            "Erro origem: %s. Key interval: %s-%s",
                            str(e),
                            key_begin,
                            key_end,
                        )

                        return False, key_begin

                    last_sleep = datetime.now()
                    run_step = 60 * 30  # 30 minutos

                    # Consulta max id na origem
                    db_hook = PostgresHook(postgres_conn_id=source_conn_id)
                    max_id_sql = f"""select COALESCE(max({key_column}),0)
                                        FROM {source_table}"""
                    max_id = int(db_hook.get_first(max_id_sql)[0])

                    # while rows:
                    while key_begin <= max_id and max_id > 0:
                        if (datetime.now() - last_sleep).seconds > run_step:
                            logging.info(
                                "Roda por %d segundos e dorme por 20 segundos para evitar o erro Negsignal.SIGKILL do Airflow!",
                                run_step,
                            )
                            time.sleep(20)
                            last_sleep = datetime.now()

                        # Tenta ESCREVER no destino
                        if rows:
                            try:
                                destination_cur.executemany(insert, rows)
                                destination_conn.commit()
                            except Exception as e:
                                logging.info(
                                    "Erro destino: %s. Key interval: %s-%s",
                                    str(e),
                                    key_begin,
                                    key_end,
                                )

                                return False, key_begin

                        rows_inserted += len(rows)
                        key_begin = key_end + 1
                        key_end = key_begin + key_interval - 1
                        # Tenta LER na origem
                        try:
                            # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchall()
                            source_cur.execute(select_sql, (key_begin, key_end))
                            rows = source_cur.fetchall()  # psycopg2
                        except Exception as e:
                            logging.info(
                                "Erro origem: %s. Key interval: %s-%s",
                                str(e),
                                key_begin,
                                key_end,
                            )

                            return False, key_begin

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time
                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)

                    return True, None


def copy_by_key_with_retry(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 10000,
    destination_truncate: bool = True,
    retries: int = 0,
    retry_delay: int = 600,
):
    """
    Copia tabela entre dois bancos de dados chamando a function
    copy_by_key_interval(), mas permitindo retries quando ocorre falha.
    Uso: cópia de tabelas grandes do Quartzo em que se verifica frequentes
    falhas durante os fetchs, por exemplo, no vdb Siasg.
    A vantagem dos retries da function é não precisar fazer count na tabela
    destino a cada nova tentativa de restart. Em tabelas grandes, o count
    demora para retornar.

    Exemplo:
        copy_by_key_with_retry(
                            source_provider='PG',
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_provider='MSSQL',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            key_column='id',
                            key_start=8150500,
                            key_interval=10000,
                            destination_truncate=False,
                            retries=10,
                            retry_delay=300)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a cópia é iniciada
        key_interval (int): intervalo de id's para ler da origem a cada vez
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
        retries (int): número máximo de tentativas de reprocessamento
        retry_delay (int): quantos segundos aguardar antes da nova tentativa
    """

    retry = 0
    succeeded, next_key = copy_by_key_interval(
        source_provider=source_provider,
        source_conn_id=source_conn_id,
        source_table=source_table,
        destination_provider=destination_provider,
        destination_conn_id=destination_conn_id,
        destination_table=destination_table,
        key_column=key_column,
        key_start=key_start,
        key_interval=key_interval,
        destination_truncate=destination_truncate,
    )

    while not succeeded and (retry <= retries):
        logging.info("Falha na function copy_by_key_interval !!!")
        retry += 1
        logging.info("Tentando retry %d em %d segundos...", retry, retry_delay)
        time.sleep(retry_delay)
        succeeded, next_key = copy_by_key_interval(
            source_provider=source_provider,
            source_conn_id=source_conn_id,
            source_table=source_table,
            destination_provider=destination_provider,
            destination_conn_id=destination_conn_id,
            destination_table=destination_table,
            key_column=key_column,
            key_start=next_key,
            key_interval=key_interval,
            destination_truncate=False,
        )

    if succeeded:
        logging.info("Término com sucesso!")
    else:
        logging.info("Término com erro após %d tentativas!", retries)


def copy_by_limit_offset(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    limit: int = 1000,
    destination_truncate: bool = True,
):
    """
    Carrega dado do Postgres/MSSQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas já existentes na tabela de destino, usando
    blocagem de linhas limit/offset na consulta à origem.
    Tabela de destino deve ter as mesmas colunas ou subconjunto das colunas
    da tabela de origem, e com data types iguais ou compatíveis.

    Exemplo:
        copy_by_limit_offset(
                            source_provider='PG',
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_provider='MSSQL',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            limit=1000,
                            destination_truncate=True)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        limit (int): quantidade de linhas para ler da origem a cada vez
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
    """

    # validate db string
    validate_db_string(source_table, destination_table, None)

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:

                    # Fast etl
                    destination_conn.autocommit = False
                    destination_cur.fast_executemany = True

                    # gera queries com limit e offset
                    col_list = get_cols_name(
                        destination_cur, destination_provider, destination_table
                    )
                    insert, truncate = build_dest_sqls(destination_table, col_list, "?")
                    select_sql = build_select_sql(source_table, col_list)
                    # pyodbc: select_sql = f"{select_sql} limit ?, ?"
                    select_sql = f"{select_sql} limit %s, %s"

                    # truncate stg
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        destination_cur.commit()

                    # copy by limit offset
                    start_time = time.perf_counter()
                    next_offset = 0
                    rows_inserted = 0
                    # pyodbc: rows = source_cur.execute(select_sql, next_offset, limit).fetchall()
                    source_cur.execute(select_sql, (next_offset, limit))
                    rows = source_cur.fetchall()  # psycopg2

                    while rows:
                        destination_cur.executemany(insert, rows)
                        destination_conn.commit()
                        rows_inserted += len(rows)
                        next_offset = next_offset + limit
                        # pyodbc: rows = source_cur.execute(select_sql, next_offset, limit).fetchall()
                        source_cur.execute(select_sql, (next_offset, limit))
                        rows = source_cur.fetchall()  # psycopg2

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time
                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)


def search_key_gaps(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 100,
):
    """
    Verifica se existem lacunas de linhas entre intervalos de chaves,
    comparando tabela origem x tabela destino. Para cada intervalo, deve
    existir a mesma quantidade distinta de id's.

    Exemplo:
        search_key_gaps(
                        source_provider='PG',
                        source_conn_id='quartzo_comprasnet',
                        source_table='Comprasnet_VBL.tbl_pregao',
                        destination_provider='MSSQL',
                        destination_conn_id='mssql_srv_32_stg_comprasnet',
                        destination_table='dbo.tbl_pregao',
                        key_column='prgCod',
                        key_start=0,
                        key_interval=100000)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a comparação é feita
        key_interval (int): intervalo de id's para ler da origem a cada vez
    """
    # validate db string
    validate_db_string(source_table, destination_table, None)

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:

                    # gera queries
                    select_sql = f"""
                        SELECT COUNT(DISTINCT {key_column}) AS count_source
                        FROM {source_table}"""
                    # pyodbc: select_sql = f"{select_sql} WHERE {key_column} BETWEEN ? AND ?"
                    select_sql = f"{select_sql} WHERE {key_column} BETWEEN %s AND %s"
                    compare_sql = f"""
                        SELECT COUNT(DISTINCT {key_column}) AS count_dest
                        FROM {destination_table}"""
                    compare_sql = f"{compare_sql} WHERE {key_column} BETWEEN ? AND ?"
                    # psycopg2: compare_sql = f"{compare_sql} WHERE {key_column} BETWEEN %s AND %s"
                    lastkey_sql = f"""
                        SELECT MAX({key_column}) AS last_key
                        FROM {destination_table}"""

                    # compare by key interval
                    start_time = time.perf_counter()

                    rowlast = destination_cur.execute(lastkey_sql).fetchone()
                    # psycopg2: destination_cur.execute(lastkey_sql)
                    # psycopg2: rowlast = destination_cur.fetchone()
                    last_key = rowlast.last_key

                    gaps = totdif = 0
                    key_begin = key_start
                    key_end = key_begin + key_interval - 1
                    # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchone()
                    source_cur.execute(select_sql, (key_begin, key_end))
                    rows = source_cur.fetchone()

                    while rows:
                        rowsdest = destination_cur.execute(
                            compare_sql, key_begin, key_end
                        ).fetchone()
                        # psycopg2: destination_cur.execute(compare_sql, (key_begin, key_end))
                        # psycopg2: rowsdest = destination_cur.fetchone()
                        logging.info(
                            "Key interval: %d to %d. %s",
                            key_begin,
                            key_end,
                            time.strftime("%H:%M:%S", time.localtime()),
                        )
                        # pyodbc: count_source = rows.count_source
                        count_source = rows[0]  # psycopg2
                        if count_source != rowsdest.count_dest:
                            dif = count_source - rowsdest.count_dest
                            logging.info(
                                "Gap!!! Source keys: %d. Dest keys: %d. Difference: %d.",
                                count_source,
                                rowsdest.count_dest,
                                dif,
                            )
                            gaps += 1
                            totdif += dif
                        key_begin = key_end + 1
                        key_end = key_begin + key_interval - 1
                        if key_begin > last_key:
                            break
                        # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchone()
                        source_cur.execute(select_sql, (key_begin, key_end))
                        rows = source_cur.fetchone()

                    delta_time = time.perf_counter() - start_time
                    logging.info("Tempo do compare: %f segundos", delta_time)
                    logging.info(
                        "Resumo: %d gaps, %d rows faltam no destino!", gaps, totdif
                    )


def load_env_var(conn_name: str, conn_id: str):
    """
    Atribui a string de conexão para uma variável de ambiente em execução.
    Útil para executar o Great Expectations através de uma DAG, utilizando
    as credenciais das connections do próprio airflow e referenciando
    nas variáveis setadas no arquivo `great_expectations.yml`.

    Exemplo:
        load_env_var("pgg_stage", "mssql_srv_30_pgg_stage")

    Args:
        conn_name (str): Identificador da conexão
        conn_id (str): connection origem do Airflow
    """

    conn_values = BaseHook.get_connection(conn_id)

    if conn_values.conn_type == "mssql":
        connection_string = get_mssql_odbc_conn_str(conn_id)
    elif conn_values.conn_type == "postgres":
        connection_string = PostgresHook(conn_id).get_uri()
    elif conn_values.conn_type == "mysql":
        connection_string = MySqlHook(conn_id).get_uri()

    # Grava o valor em variavel de ambiente na execução
    os.environ[conn_name] = connection_string
