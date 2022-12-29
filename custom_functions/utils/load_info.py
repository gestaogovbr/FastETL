"""
Obtém informações sobre tabelas carregadas e grava em tabela de log.
"""

from airflow.hooks.base import BaseHook

class LoadInfo:
    def __init__(self, source_conn_id: str,
                    source_schema_table: str,
                    load_type: str,
                    dest_conn_id: str,
                    log_schema_name: str,
                    log_table_name: str = "consumo_dados") -> None:
        self.s_conn_id = source_conn_id
        self.s_schema, self.s_table = source_schema_table.split(".")
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
        Cria a tabela de log caso ela não exista.
        """
        conn_values = BaseHook.get_connection(self.d_conn_id)
        if conn_values.conn_type == "mssql":
            create_prefix = f"""
                    IF OBJECT_ID('{self.log_schema_name}.{self.log_table_name}',
                        'U') IS NULL CREATE TABLE
                    """
            date_type = "datetime2"
        elif conn_values.conn_type == "postgres":
            create_prefix = "CREATE TABLE IF NOT EXISTS "
            date_type = "timestamp"

        else:
            raise Exception("Conn_type not implemented.")

        sql = f"""{create_prefix}
                {self.log_schema_name}.{self.log_table_name} (
                no_vdb      varchar(30) NOT NULL,
                no_schema   varchar(30) NOT NULL,
                no_tabela   varchar(60) NOT NULL,
                no_usuario  varchar(20) NOT NULL,
                tp_carga    varchar(15) NOT NULL,
                dt_consumo  {date_type} NOT NULL,
                qt_linhas   bigint NULL)
                """
        db_hook = conn_values.get_hook()
        db_hook.run(sql)

    def write(self, rows_loaded: int):
        """
        Insere na tabela de log as informações das linhas carregadas.
        """
        self.create_log_table()
        sql = f"""
        INSERT INTO {self.log_schema_name}.{self.log_table_name}
            (no_vdb, no_schema, no_tabela, no_usuario, tp_carga, dt_consumo, qt_linhas)
        VALUES ('{self.s_conn_database}', '{self.s_schema}', '{self.s_table}',
                '{self.s_conn_login}', '{self.load_type}', CURRENT_TIMESTAMP, {rows_loaded})
        """
        conn_values = BaseHook.get_connection(self.d_conn_id)
        db_hook = conn_values.get_hook()
        db_hook.run(sql)
