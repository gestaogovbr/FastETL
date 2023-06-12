"""
Class and functions for Teiid Database Server
"""


import pyodbc
import yaml
import os
import pandas as pd
from typing import Tuple
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from fastetl.custom_functions.fast_etl import SourceConnection, DestinationConnection
from fastetl.custom_functions.fast_etl import get_conn_type
# class Teiid:
#     """
#     Gera as conexões de origem do Quartzo (Teiid)
#     """

#     def __init__(self, conn_id: str):
#         self.conn_type = get_conn_type(conn_id)
#         self.conn = None
#         self.hook, _ = get_hook_and_engine_by_provider(conn_id)


def check_is_teiid(conn_id):
    sql = "SELECT * FROM SYS.Tables WHERE 1=2"
    hook = PostgresHook(conn_id)
    check = hook.get_first(sql)[0]
    if check:
        return True

    return False


def _get_tables_n_columns_from_teiid(conn_id) -> pd.DataFrame:
    """
    Pega uma lista de metadados das colunas do banco quartzo.
    """

    pg_hook = PostgresHook(conn_id)
    rows = pg_hook.get_pandas_df(
        f"""SELECT
                TableName,
                Name,
                DataType,
                Scale,
                Length,
                IsLengthFixed,
                "Precision",
                Description
            FROM
                SYS.Columns
            WHERE
                VDBName = '{vdb}'
                and SchemaName = '{vdb}_VBL'
                and TableName IN ('{tables}')
        """
    )

    rows.replace({'"': "", "'": ""}, regex=True, inplace=True)

    return rows

def map_datatypes(mapping: dict, row: pd.Series, destination: str) -> pd.Series:


    #row["DataType"] = mapping["teiid"][destination].get(row["DataType"], row["DataType"])

    if row['DataType'] in mapping["teiid"][destination]:




        rows.loc[index] = row

    return row

def create_table(source: SourceConnection, destination: DestinationConnection):
    df_source_columns = _get_tables_n_columns_from_teiid(source.conn_id)

    df_source_columns["new_width"] = None
    current_path = os.path.dirname(__file__)

    mapping = yaml.safe_load(
        open(os.path.join(current_path, "config", "types_mapping.yml"))
    )
    destination_provider = get_conn_type(destination.conn_id)

    df_dest_columns = df_source_columns.apply(map_datatypes, axis=1, mapping=mapping, destination=destination_provider)



# def __exit__(self, exc_type, exc_value, traceback):
#     self.conn.close()
