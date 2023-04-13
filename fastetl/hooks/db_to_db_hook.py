"""
Execute table data transfer operations between two databases Postgres
and MSSQL. These functions are building blocks of Operators that copy
data following full and incremental strategies.
"""

from datetime import datetime
from typing import Dict

from airflow.hooks.base import BaseHook

from fastetl.custom_functions.fast_etl import copy_db_to_db, sync_db_2_db


class DbToDbHook(BaseHook):

    def __init__(
        self,
        source: Dict[str, str],
        destination: Dict[str, str],
        *args,
        **kwargs
    ):
        self.source = source
        self.destination = destination

    def full_copy(
        self,
        columns_to_ignore: list = None,
        destination_truncate: str = True,
        chunksize: int = 1000,
        copy_table_comments: bool = False,
    ):
        copy_db_to_db(
            source=self.source,
            destination=self.destination,
            columns_to_ignore=columns_to_ignore,
            destination_truncate=destination_truncate,
            chunksize=chunksize,
            copy_table_comments=copy_table_comments,
        )

    def incremental_copy(
        self,
        table: str,
        date_column: str,
        key_column: str,
        since_datetime: datetime = None,
        sync_exclusions: bool = False,
        chunksize: int = 1000,
        copy_table_comments: bool = False,
    ):
        sync_db_2_db(
            source_conn_id=self.source.conn_id,
            destination_conn_id=self.destination.conn_id,
            source_schema=self.source.schema,
            source_exc_schema=self.source.get("source_exc_schema", None),
            source_exc_table=self.source.get("source_exc_table", None),
            source_exc_column=self.source.get("source_exc_column", None),
            select_sql=self.source.get("query", None),
            destination_schema=self.destination.schema,
            increment_schema=self.destination.get("increment_schema", None),
            table=table,
            date_column=date_column,
            key_column=key_column,
            since_datetime=since_datetime,
            sync_exclusions=sync_exclusions,
            chunksize=chunksize,
            copy_table_comments=copy_table_comments,
        )
