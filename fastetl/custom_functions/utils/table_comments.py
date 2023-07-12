"""
Module to copy table and columns comments/descriptions from source to
target. Works between Postgres, MySql, MsSql.
"""

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError
from alembic.migration import MigrationContext
from alembic.operations import Operations

from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from fastetl.custom_functions.utils.db_connection import (
    get_hook_and_engine_by_provider,
    get_conn_type,
)
from fastetl.custom_functions.utils.get_table_cols_name import (
    get_table_cols_name,
)


class TableComments:
    """
    Retrieve and save table comments/descriptions (including columns) from one
    database to another. Accepts on the origin teiid, mssql and postgres.
    And accepts to the destination mssql and postgres.
    """

    def __init__(self, conn_id: str, schema: str, table: str):
        """Initialize TableComments class variables.

        Args:
            conn_id (str): Airflow connection id
            schema (str): Schema str
            table (str): Table str
        """

        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        conn_values = BaseHook.get_connection(conn_id)
        self.conn_type = get_conn_type(conn_id)
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
            table_comments = pd.concat(
                [table_comments, rows_df], ignore_index=True
            )

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

        _, engine = get_hook_and_engine_by_provider(conn_id=self.conn_id)
        inspector = inspect(engine)

        table_info = inspector.get_table_comment(
            table_name=self.table, schema=self.schema
        )
        table_comments = self.table_comments_init.copy()

        table_comments = pd.concat(
            [
                table_comments,
                pd.DataFrame(
                    {
                        "database_level": ["table"],
                        "name": [self.table],
                        "comment": [table_info["text"]],
                    }
                ),
            ],
            ignore_index=True,
        )

        columns_info = inspector.get_columns(
            table_name=self.table, schema=self.schema
        )
        for row in columns_info:
            table_comments = pd.concat(
                [
                    table_comments,
                    pd.DataFrame(
                        {
                            "database_level": ["column"],
                            "name": [row["name"]],
                            "comment": [row["comment"]],
                        }
                    ),
                ],
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
                columns={"Name": "name", "Description": "comment"},
                inplace=True,
            )
            rows_df["database_level"] = database_level
            table_comments = pd.concat(
                [table_comments, rows_df], ignore_index=True
            )

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
            comment = pd.Series(dtype="object")

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

        _, engine = get_hook_and_engine_by_provider(conn_id=self.conn_id)
        conn = engine.connect()
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        # Part 1 - write table comment

        comment = self._get_comment_value(database_level="table")

        if not comment.empty:
            op.create_table_comment(
                table_name=self.table,
                schema=self.schema,
                comment=comment.values[0],
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
            try:
                table_comments = self._get_pg_table_comments()
            except OperationalError:
                table_comments = self._get_teiid_table_comments()
        elif self.conn_type == "teiid":
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
