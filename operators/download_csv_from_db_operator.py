"""
Operador que executa uma query SQL, gera um arquivo CSV com o resultado
e grava o arquivo no sistema de arquivo.

Args:
    conn_id (str): Airflow conn_id do BD onde a query select_sql
    será executada
    select_sql (str): query que retorna os dados que serão gravados no
    CSV
    table_name (str): nome da tabela utilizado para construção dinâmica
    do sql. Deve ser utilizado alternativamente ao parâmetro `select_sql`
    table_scheme (str): nome do esquema utilizado para construção
    dinâmica do sql. Deve ser utilizado alternativamente ao parâmetro
    `select_sql` em conjunto com o `table_name`
    columns_to_remove (list): deve ser utilizado em conjunto com os
    campos `table_name` e `table_scheme` para remover as colunas que
    não serão extraídas para o CSB
    target_file_dir (str): local no sistema de arquivo onde o arquivo
    CSV será gravado
    file_name (str): nome para o arquivo CSV a ser gravado
    int_columns (str): lista com nome das colunas que são do tipo
    inteiro para geração correta do arquivo CSV
"""

import os

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from FastETL.custom_functions.fast_etl import get_table_cols_name

class DownloadCSVFromDbOperator(BaseOperator):
    ui_color = '#95aad5'
    ui_fgcolor = '#000000'
    template_fields = ('select_sql', 'target_file_dir', 'file_name')

    @apply_defaults
    def __init__(self,
                 conn_id,
                 target_file_dir,
                 file_name,
                 select_sql=None,
                 table_name=None,
                 table_scheme=None,
                 columns_to_remove: []=None,
                 int_columns=None,
                 *args,
                 **kwargs
                 ):
        super(DownloadCSVFromDbOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.select_sql = select_sql
        self.table_name = table_name
        self.table_scheme = table_scheme
        self.columns_to_remove = columns_to_remove
        self.int_columns = int_columns
        self.target_file_dir = target_file_dir
        self.file_name = file_name

    def select_all_sql(self):
        cols = get_table_cols_name(self.conn_id, self.table_scheme, self.table_name)
        if self.columns_to_remove:
            cols = [c for c in cols if c not in self.columns_to_remove]

        return f"""
            SELECT
            {','.join(cols)}
            FROM {self.table_scheme}.{self.table_name};
            """

    def execute(self, context):
        base_hook = BaseHook.get_connection(self.conn_id)

        if base_hook.conn_type == 'mssql':
            db_hook = MsSqlHook(mssql_conn_id=self.conn_id)
        elif base_hook.conn_type == 'postgres':
            db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        else:
            raise Exception('Conn_type not implemented.')

        df = db_hook.get_pandas_df(
            self.select_sql if self.select_sql
            else self.select_all_sql()
            )

        # Convert columns data types to int
        if self.int_columns:
            for col in self.int_columns:
                df[col] = df[col].astype("Int64")

        # Create folder if not exists
        if not os.path.exists(self.target_file_dir):
            os.mkdir(self.target_file_dir)

        file_path = os.path.join(self.target_file_dir, self.file_name)
        df.to_csv(file_path, index=False)
        return self.file_name
