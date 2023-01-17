import os

from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook

from FastETL.custom_functions.fast_etl import get_mssql_odbc_conn_str

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