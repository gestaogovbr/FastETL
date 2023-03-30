"""
Operador que realiza a cópia de dados seguindo uma estratégia completa
(full) entre de um banco de dados para outro. Os BDs podem ser Postgres
ou SQL Server. Internamente são utilizadas as bibliotecas psycopg2 e
pyodbc. Os dados copiados podem ser oriundos de uma tabela ou de um
Select SQL.

Args:
    destination_table (str): tabela de destino no formato schema.table
    source_conn_id (str): connection origem do Airflow
    destination_conn_id (str): connection destino do Airflow
    source_table (str): tabela de origem no formato schema.table
    select_sql (str): query sql para consulta na origem. Se utilizado o
    source_table será ignorado
"""

from datetime import datetime
from typing import Dict

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from FastETL.hooks.db_to_db_hook import DbToDbHook

class CopyDbToDbOperator(BaseOperator):

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
