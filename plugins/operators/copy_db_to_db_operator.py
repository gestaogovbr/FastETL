
"""
   2020
   Deckers
   Ministério da Economia / SEGES / CGINF
   
   Operador customizado piloto que copia tabela de um banco para outro entre MSSQL's para MSSQL's ou 
   entre Postgres para Postgres ou entre MSSQL's para Postgres ou vice e versa.
   
    Carrega dado do Postgres/MSSQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas e linhas já existentes na tabela de destino.
    Tabela de destino deve ter a mesma estrutura e nome de tabela e colunas
    que a tabela de origem, ou passar um select_sql que tenha as colunas de
    destino.
   
   Args: 
        destination_table (str): tabela de destino no formato schema.table
        source_conn_id (str): connection origem do Airflow
        source_provider (str): provider do banco origem (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        source_table (str): tabela de origem no formato schema.table
        select_sql (str): query sql para consulta na origem. Se utilizado o
        source_table será ignorado

   
"""

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from custom_functions.fast_etl import copy_db_to_db

class CopyDbToDbOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            destination_table: str,
            source_conn_id: str,
            source_provider: str,
            destination_conn_id: str,
            destination_provider: str,
            source_table: str = None,
            select_sql: str = None,
            destination_truncate: bool = True,
            chunksize: int = 1000,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.source_provider = source_provider
        self.destination_conn_id = destination_conn_id
        self.destination_provider = destination_provider
        self.source_table = source_table
        self.select_sql = select_sql
        self.destination_truncate = destination_truncate
        self.chunksize = chunksize
       

    def execute(self, context):
        copied = copy_db_to_db(
            destination_table=self.destination_table,
            source_conn_id=self.source_conn_id,
            source_provider=self.source_provider,
            destination_conn_id=self.destination_conn_id,
            destination_provider=self.destination_provider,
            source_table=self.source_table,
            select_sql=self.select_sql)
        message = "logs {}".format(copied)
        print(message)
        return message