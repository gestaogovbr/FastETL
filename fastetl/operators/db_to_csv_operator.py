"""
Module contains the class

DbToCSVOperator
    Operador that executes a SQL query, generates a CSV file with the
    result and stores the file in the file system.
"""

from typing import Optional
import os

from airflow.models.baseoperator import BaseOperator

from fastetl.custom_functions.utils.get_table_cols_name import get_table_cols_name
from fastetl.custom_functions.utils.db_connection import get_hook_and_engine_by_provider

class DbToCSVOperator(BaseOperator):
    """
    Operador that executes a SQL query, generates a CSV file with the
    result and stores the file in the file system.

    Args:
        conn_id (str): Airflow conn_id of the database where the
            `select_sql` query will be run.
        target_file_dir (str): path to the directory where the target
            file will be created.
        file_name (str): name of the file to be created.
        compression (str | dict): compression parameter to be passed
            along to Panda's to_csv method. Defaults to "infer".
        select_sql (Optional[str]): query string, or path to a file
            containing the query string, that will select and return the
            data to be recorded in the CSV file. If omitted (or None),
            will build a select query containing the specified columns.
            Defaults to None.
        table_name (Optional[str]): name of the table to dynamically build
            the query. Must be used alternatively to the `select_sql`
            argument. Defaults to None.
        table_scheme (Optional[str]): name of the schema to be used to
            dynamically build the query. Must be used alternatively to
            the `select_sql` argument in tandem with the `table_name`
            argument. Defaults to None.
        characters_to_remove (Optional[str]): if specified, the characters
            specified in this string will be removed from any string type
            columns in the dataframe, before exporting to CSV. Defaults
            to None.
        columns_to_remove (Optional[list[str]]): must be used together with
            the `table_name` and `table_scheme` arguments to except the
            columns which won't be extracted to the CSV. Defaults to None.
        int_columns (Optional[list[str]]): list with the names of the
            columns that are of type integer to generate the CSV file
            correctly. Defaults to None.
    """
    ui_color = '#95aad5'
    ui_fgcolor = '#000000'
    template_fields = ('select_sql', 'target_file_dir', 'file_name')

    def __init__(self,
                 *args,
                 conn_id: str,
                 target_file_dir: str,
                 file_name: str,
                 compression: str | dict = "infer",
                 select_sql: Optional[str] = None,
                 table_name: Optional[str] = None,
                 table_scheme: Optional[str] = None,
                 characters_to_remove: Optional[str] = None,
                 columns_to_remove: Optional[list[str]] = None,
                 int_columns: Optional[list[str]] = None,
                 **kwargs
                 ):
        super(DbToCSVOperator, self).__init__(*args, **kwargs)
        self.conn_id: str = conn_id
        self.target_file_dir: str = target_file_dir
        self.file_name: str = file_name
        self.compression: str | dict = compression
        self.select_sql: Optional[str] = select_sql
        self.table_name: Optional[str] = table_name
        self.table_scheme: Optional[str] = table_scheme
        self.characters_to_remove: Optional[str] = characters_to_remove
        self.columns_to_remove: Optional[list[str]] = columns_to_remove
        self.int_columns: Optional[list[str]] = int_columns

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
