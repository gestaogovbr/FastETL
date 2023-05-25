"""
Airflow operator that performs data copying between DBs with complete
or incremental strategy. The DBs can be Postgres, SQL Server or MySQL.
It uses psycopg2 and pyodbc libraries. The copied data can
come from a table or an SQL Query.

Args:
    is_incremental (bool, optional): Whether to perform an incremental
        copy based on a datetime or key column. Defaults to False.

    (when full copy)
    columns_to_ignore (List[str], optional): A list of column names to
        ignore during the copy operation. Defaults to None.
    destination_truncate (bool, optional): Whether to truncate the
        destination table before copying data to it. Defaults to True.

    (when incremental copy)
    table (str, optional): The name of the table to copy in incremental
        mode. Defaults to None.
    date_column (str, optional): The name of the datetime column to use for
        incremental copying. Defaults to None.
    key_column (str, optional): The name of the key column to use for
        incremental copying. Defaults to None.
    since_datetime (datetime.datetime, optional): The datetime from
        which to start the incremental copy. Defaults to None.
    sync_exclusions (bool, optional): Whether to exclude some columns during
        incremental copying. Defaults to False.

    (both full and incremental)
    source (Dict[str, str]): A dictionary containing the connection details
        of the source database.

        Depending on full or incremental copy, specific keys can be passed
        on the source dictionary.

        (full copy)
        source full copy dict expects these keys:
        * conn_id -> required
        * schema and table -> required if `query` not provided.
        * query -> required if `schema` and `table` not provided.

        (incremental copy)
        source incremental copy dict expects these keys:
        * conn_id -> required
        * schema -> required
        * query -> optional
        * source_exc_schema -> optional
            Table `schema` name at the source where exclusions are recorded.
        * source_exc_table -> optional
            Table `table` name at the source where exclusions are recorded.
        * source_exc_column -> optional
            Table `column` name at the source where exclusions are recorded.

    destination (Dict[str, str]): A dictionary containing the connection
        details of the destination database.

        Depending on full or incremental copy, specific keys can be passed
        on the destination dictionary.

        (full copy)
        destination full copy dict expects these keys:
        * conn_id -> required
        * schema -> required
        * table -> required

        (incremental copy)
        source incremental copy dict expects these keys:
        * conn_id -> required
        * schema -> required
        * increment_schema -> optional
            Schema in the database used for temporary tables. If this
            variable is None, this table will be created in the same
            schema with the suffix '_alteracoes'.

    chunksize (int, optional): The number of rows to fetch from the source
        database at once. Defaults to 1000.
    copy_table_comments (bool, optional): Whether to copy table comments
        from the source database to the destination database.
        Defaults to False.

Raises:
    TypeError: If `source` or `destination` is not a dictionary.
"""

from datetime import datetime
from typing import Dict

from airflow.models.baseoperator import BaseOperator

from fastetl.hooks.db_to_db_hook import DbToDbHook

class DbToDbOperator(BaseOperator):
    template_fields = ['source']

    def __init__(
            self,
            source: Dict[str, str],
            destination: Dict[str, str],
            columns_to_ignore: list = None,
            destination_truncate: bool = True,
            chunksize: int = 1000,
            copy_table_comments: bool = False,
            is_incremental: bool = False,
            table: str = None,
            date_column: str = None,
            key_column: str = None,
            since_datetime: datetime = None,
            sync_exclusions: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source = source
        self.destination = destination
        self.columns_to_ignore = columns_to_ignore
        self.destination_truncate = destination_truncate
        self.chunksize = chunksize
        self.copy_table_comments = copy_table_comments
        self.is_incremental=is_incremental
        self.table=table
        self.date_column=date_column
        self.key_column=key_column
        self.since_datetime=since_datetime
        self.sync_exclusions=sync_exclusions

    def execute(self, context):
        hook = DbToDbHook(
            source=self.source,
            destination=self.destination,
            )

        if self.is_incremental:
            hook.incremental_copy(
                table=self.table,
                date_column=self.date_column,
                key_column=self.key_column,
                since_datetime=self.since_datetime,
                sync_exclusions=self.sync_exclusions,
                chunksize=self.chunksize,
                copy_table_comments=self.copy_table_comments,
            )
        else:
            hook.full_copy(
                columns_to_ignore=self.columns_to_ignore,
                destination_truncate=self.destination_truncate,
                chunksize=self.chunksize,
                copy_table_comments=self.copy_table_comments
            )
