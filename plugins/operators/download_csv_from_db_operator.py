"""
TODO: Escrever essa docstring
"""

import os

from airflow.operators.bash_operator import BaseOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.utils.decorators import apply_defaults


class DownloadCSVFromDbOperator(BaseOperator):
    ui_color = '#95aad5'
    ui_fgcolor = '#000000'

    @apply_defaults
    def __init__(self,
                 mssql_conn_id,
                 select_sql,
                 target_file_path,
                 file_name,
                 int_columns=None,
                 *args,
                 **kwargs
                 ):
        super(DownloadCSVFromDbOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.select_sql = select_sql
        self.int_columns = int_columns
        self.target_file_path = target_file_path
        self.file_name = file_name

    def execute(self, context):
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        df = mssql_hook.get_pandas_df(self.select_sql)

        # Convert columns data types to int
        if self.int_columns:
            for col in self.int_columns:
                df[col] = df[col].astype("Int64")

        # Create folder if not exists
        if not os.path.exists(self.target_file_path):
            os.mkdir(self.target_file_path)

        file_path = os.path.join(self.target_file_path, self.file_name)
        df.to_csv(file_path, index=False)
