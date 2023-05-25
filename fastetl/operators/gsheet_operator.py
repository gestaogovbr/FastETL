"""
Operador Customizado que carrega uma planilha do Google em uma tabela no
banco de dados. A primera linha da planilha é utilizada para nomear as
colunas. Caso a tabela não exista ela será criada automaticamente.
Permite a inclusão de uma coluna com valor constante. Funciona com MSSql
Server.

Args:
    gsheet_conn_id (str): conn_id contendo JSON google authentication no
    campo password
    spreadsheet_id (str): id único da spreadsheet google. Faz parte da
    URL da planilha online
    sheet_name (str): nome da planilha dentre as várias da spreadsheet
    que será carregada na tabela
    dest_conn_id (str): conn_id para o BD de escrita dos dados
    schema (str): schema da tabela a ser escrita
    table (str): tabela a ser escrita
    column_name_to_add (str): nome da coluna a ser incluída na tabela.
    Por padrão não adicionar nenhuma coluna
    value_to_add : valor a ser incluído caso se utilize o parâmetro
    anterior
"""

from airflow.models.baseoperator import BaseOperator

from fastetl.hooks.gsheet_hook import GSheetHook
from fastetl.custom_functions.utils.db_connection import get_mssql_odbc_engine

class GSheetToDbOperator(BaseOperator):
    ui_color = '#72efdd'
    ui_fgcolor = '#000000'
    template_fields = ('sheet_name', 'column_name_to_add', 'value_to_add' )

    def __init__(self,
                 gsheet_conn_id,
                 spreadsheet_id,
                 sheet_name,
                 dest_conn_id,
                 schema,
                 table,
                 column_name_to_add=None,
                 value_to_add=None,
                 *args,
                 **kwargs):
        super(GSheetToDbOperator, self).__init__(*args, **kwargs)
        self.gsheet_conn_id = gsheet_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.dest_conn_id = dest_conn_id
        self.schema = schema
        self.table = table
        self.column_name_to_add = column_name_to_add
        self.value_to_add = value_to_add

    def execute(self, context):
        gsheet_hook = GSheetHook(conn_id=self.gsheet_conn_id,
                                 spreadsheet_id=self.spreadsheet_id)
        df = gsheet_hook.get_gsheet_df(sheet_name=self.sheet_name)

        # Remove colunas vazias
        df = df[[c for c in df.columns if len(c) > 0]]

        if self.column_name_to_add:
            df[self.column_name_to_add] = self.value_to_add

        df.to_sql(self.table,
                  schema=self.schema,
                  con=get_mssql_odbc_engine(self.dest_conn_id),
                  if_exists='append',
                  index=False)

class GSheetToCSVOperator(BaseOperator):
    ui_color = '#72efdd'
    ui_fgcolor = '#000000'

    def __init__(self,
                 gsheet_conn_id,
                 spreadsheet_id,
                 sheet_name,
                 dest_path,
                 *args,
                 **kwargs):
        super(GSheetToCSVOperator, self).__init__(*args, **kwargs)
        self.gsheet_conn_id = gsheet_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.dest_path = dest_path

    def execute(self, context):
        gsheet_hook = GSheetHook(conn_id=self.gsheet_conn_id,
                                 spreadsheet_id=self.spreadsheet_id)
        df = gsheet_hook.get_gsheet_df(sheet_name=self.sheet_name)

        # Remove colunas vazias
        df = df[[c for c in df.columns if len(c) > 0]]

        df.to_csv(self.dest_path, index=False)
