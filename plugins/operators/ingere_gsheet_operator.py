from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

from custom_functions.gsheet_services import get_gsheet_df
from custom_functions.fast_etl import get_mssql_odbc_engine

class IngereGSheetOperator(BaseOperator):
    ui_color = '#72efdd'
    ui_fgcolor = '#000000'

    @apply_defaults
    def __init__(self,
                 gsheet_conn_id,
                 spreadsheet_id,
                 sheet_name,
                 dest_conn_id,
                 schema,
                 table,
                 column_name_to_add,
                 value_to_add,
                  *args,
                  **kwargs):
        super(IngereGSheetOperator, self).__init__(*args, **kwargs)
        self.gsheet_conn_id = gsheet_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.dest_conn_id = dest_conn_id
        self.schema = schema
        self.table = table
        self.column_name_to_add = column_name_to_add
        self.value_to_add = value_to_add

    def execute(self, context):
        df = get_gsheet_df(conn_id=self.gsheet_conn_id,
                           spreadsheet_id=self.spreadsheet_id,
                           sheet_name=self.sheet_name)

        df[self.column_name_to_add] = self.value_to_add

        df.to_sql(self.table,
            schema=self.schema,
            con=get_mssql_odbc_engine(self.dest_conn_id),
            if_exists='append',
            index=False)
