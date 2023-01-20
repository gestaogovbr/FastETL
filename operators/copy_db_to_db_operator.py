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

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from FastETL.hooks.db_to_db_hook import DbToDbHook

class CopyDbToDbOperator(BaseOperator):
    template_fields = ('select_sql', )

    @apply_defaults
    def __init__(
            self,
            destination_table: str,
            source_conn_id: str,
            destination_conn_id: str,
            source_table: str = None,
            select_sql: str = None,
            columns_to_ignore: list = [],
            destination_truncate: bool = True,
            chunksize: int = 1000,
            copy_table_comments: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.source_table = source_table
        self.select_sql = select_sql
        self.columns_to_ignore = columns_to_ignore
        self.destination_truncate = destination_truncate
        self.chunksize = chunksize
        self.copy_table_comments = copy_table_comments


    def execute(self, context):
        hook = DbToDbHook(
            source_conn_id=self.source_conn_id,
            destination_conn_id=self.destination_conn_id,
            )
        hook.full_copy(
            destination_table=self.destination_table,
            source_table=self.source_table,
            select_sql=self.select_sql,
            columns_to_ignore=self.columns_to_ignore,
            destination_truncate=self.destination_truncate,
            chunksize=self.chunksize,
            copy_table_comments=self.copy_table_comments
            )
