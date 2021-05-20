"""
Execute table data transfer operations between two databases Postgres
and MSSQL. These functions are building blocks of Operators that copy
data following full and incremental strategies.
"""

from airflow import settings
from airflow.models import Connection
from airflow.utils.email import send_email
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

from FastETL.custom_functions.fast_etl import copy_db_to_db

class DbToDbHook(BaseHook):

    @apply_defaults
    def __init__(self,
                 source_conn_id: str,
                 destination_conn_id: str,
                 source_provider: str,
                 destination_provider: str,
                 *args,
                 **kwargs):
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.source_provider = source_provider
        self.destination_provider = destination_provider

    def full_copy(self,
             destination_table: str,
             select_sql: str = None,
             columns_to_ignore: list = [],
             destination_truncate: str = True,
             source_table: str = None,
             chunksize: int = 1000):
        copy_db_to_db(
            source_table=source_table,
            destination_table=destination_table,
            source_conn_id=self.source_conn_id,
            source_provider=self.source_provider,
            destination_conn_id=self.destination_conn_id,
            destination_provider=self.destination_provider,
            select_sql=select_sql,
            columns_to_ignore=columns_to_ignore,
            destination_truncate=destination_truncate,
            chunksize=chunksize
            )