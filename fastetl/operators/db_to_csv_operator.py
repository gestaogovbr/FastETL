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

from fastetl.custom_functions.utils.get_table_cols_name import get_table_cols_name
from fastetl.custom_functions.utils.db_connection import get_hook_and_engine_by_provider

class DbToCSVOperator(BaseOperator):
    ui_color = '#95aad5'
    ui_fgcolor = '#000000'
    template_fields = ('select_sql', 'target_file_dir', 'file_name')

    def __init__(self,
                 conn_id,
                 target_file_dir,
                 file_name,
                 compression=None,
                 select_sql=None,
                 table_name=None,
                 table_scheme=None,
                 characters_to_remove=None,
                 columns_to_remove: []=None,
                 int_columns=None,
                 *args,
                 **kwargs
                 ):
        super(DbToCSVOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.select_sql = select_sql
        self.table_name = table_name
        self.table_scheme = table_scheme
        self.characters_to_remove = characters_to_remove
        self.columns_to_remove = columns_to_remove
        self.int_columns = int_columns
        self.target_file_dir = target_file_dir
        self.file_name = file_name
        self.compression = compression

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
        db_hook, _ = get_hook_and_engine_by_provider(self.conn_id)

        if self.select_sql:
            df_select = self.select_sql
        else:
            df_select = self.select_all_sql()

        self.log.info(f'Executing SQL check: {df_select}')
        df = db_hook.get_pandas_df(df_select)

        # Convert columns data types to int
        if self.int_columns:
            for col in self.int_columns:
                df[col] = df[col].astype("Int64")

        # Remove specified characters
        if self.characters_to_remove:
            str_cols = df.select_dtypes(['object']).columns
            for col in str_cols:
                df[col] = df[col].str.replace(self.characters_to_remove, '')

        # Remove specified columns
        if self.columns_to_remove:
            df.drop(self.columns_to_remove,
                    axis=1,
                    errors='ignore',
                    inplace=True)

        # Create folder if not exists
        if not os.path.exists(self.target_file_dir):
            os.mkdir(self.target_file_dir)

        file_path = os.path.join(self.target_file_dir, self.file_name)
        df.to_csv(file_path, index=False, compression=self.compression)
        return self.file_name
