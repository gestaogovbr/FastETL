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

from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.gsheet_hook import GSheetHook
from custom_functions.fast_etl import get_mssql_odbc_engine

class LoadGSheetOperator(BaseOperator):
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
                 column_name_to_add=None,
                 value_to_add=None,
                 *args,
                 **kwargs):
        super(LoadGSheetOperator, self).__init__(*args, **kwargs)
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

        if self.column_name_to_add:
            df[self.column_name_to_add] = self.value_to_add

        df.to_sql(self.table,
            schema=self.schema,
            con=get_mssql_odbc_engine(self.dest_conn_id),
            if_exists='append',
            index=False)
