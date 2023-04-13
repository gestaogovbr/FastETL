"""
Recieve information about table loading execution and store in log table.
"""

import logging

from airflow.hooks.base import BaseHook

from fastetl.custom_functions.utils.db_connection import get_conn_type

class LoadInfo:
    """
    Load quantity of rows downloaded into a control table.
    """

    def __init__(
        self,
        source_conn_id: str,
        source_table: str,
        load_type: str,
        dest_conn_id: str,
        log_schema_name: str,
        source_schema: str = "não informado",
        log_table_name: str = "consumo_dados",
    ) -> None:
        """Initialize LoadInfo class variables.

        Args:
            source_conn_id (str): Airflow connection id from source table
            source_schema_table (str): Source table at format `schema.table`
            load_type (str): if `incremental` or `full`
            dest_conn_id (str): Airflow connection id from destination table.
            log_schema_name (str): Name of the log's schema.
            log_table_name (str, optional): Name of the log's table.
                Defaults to "consumo_dados".
        """

        self.s_conn_id = source_conn_id
        self.s_schema = source_schema
        self.s_table = source_table
        self.load_type = load_type
        s_conn_values = BaseHook.get_connection(source_conn_id)
        self.s_conn_database = s_conn_values.schema
        self.s_conn_login = s_conn_values.login
        self.s_conn_type = s_conn_values.conn_type
        self.d_conn_id = dest_conn_id
        self.log_schema_name = log_schema_name
        self.log_table_name = log_table_name

    def create_log_table(self):
        """
        Create log table if not exists. Works for MSSql and Postgres.
        """

        d_conn_type = get_conn_type(self.d_conn_id)

        if d_conn_type == "mssql":
            create_prefix = f"""
                    IF OBJECT_ID('{self.log_schema_name}.{self.log_table_name}',
                        'U') IS NULL CREATE TABLE
                    """
            date_type = "datetime2"

        elif d_conn_type == "postgres":
            create_prefix = "CREATE TABLE IF NOT EXISTS "
            date_type = "timestamp"

        else:
            raise Exception(f"Conn_type not implemented: {d_conn_type}")

        sql = f"""{create_prefix}
                {self.log_schema_name}.{self.log_table_name} (
                no_vdb      varchar(60) NOT NULL,
                no_schema   varchar(120) NOT NULL,
                no_tabela   varchar(120) NOT NULL,
                no_usuario  varchar(40) NOT NULL,
                tp_carga    varchar(15) NOT NULL,
                dt_consumo  {date_type} NOT NULL,
                qt_linhas   bigint NULL)
                """

        db_hook = BaseHook.get_connection(self.d_conn_id).get_hook()
        db_hook.run(sql)

    def save(self, rows_loaded: int):
        """
        Save on log's table information about loaded rows.

        Args:
            rows_loaded (int): number of loaded rows.
        """

        self.create_log_table()

        sql = f"""
        INSERT INTO {self.log_schema_name}.{self.log_table_name}
            (no_vdb, no_schema, no_tabela, no_usuario, tp_carga, dt_consumo, qt_linhas)
        VALUES ('{self.s_conn_database}', '{self.s_schema}', '{self.s_table}',
                '{self.s_conn_login}', '{self.load_type}', CURRENT_TIMESTAMP, {rows_loaded})
        """

        db_hook = BaseHook.get_connection(self.d_conn_id).get_hook()
        db_hook.run(sql)
