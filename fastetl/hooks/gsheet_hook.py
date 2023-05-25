"""
Hook customizado para realizar operações de leitura e escrita com
planilhas do Google através de sua API (google sheets).
"""

import json
import os
from datetime import datetime

import pandas as pd

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from google.oauth2 import service_account
from apiclient import discovery

import pygsheets

from fastetl.custom_functions.utils.string_formatting import slugify_column_names
from fastetl.custom_functions.utils.string_formatting import convert_gsheets_str_to_datetime

class GSheetHook(BaseHook):
    """
    Hook for handling Google Spreadsheets in Apache Airflow.
    """

    def __init__(self,
                 conn_id: str,
                 spreadsheet_id: str,
                  *args,
                  **kwargs):
        self.conn_id = conn_id
        self.spreadsheet_id = spreadsheet_id

    def get_google_service(self,
        api_name: str,
        api_version: str,
        scopes: str) -> discovery.Resource:
        """
        Get a service that communicates to the Google API.

        Args:
            - api_name: The name of the api to connect to.
            - api_version: The api version to connect to.
            - scopes: A list auth scopes to authorize for the application.

        Return:
            - results (discovery.Resource): A service that is connected to the specified API.
        """

        # Transform key_id to json
        key_str = BaseHook.get_connection(self.conn_id).password
        try:
            key_value = json.loads(key_str)
        except Exception as error:
            raise Exception("Erro na leitura da conexão. Tem que copiar o "
                "conteúdo de Extra para Password.") from error

        credentials = service_account.Credentials.from_service_account_info(key_value, scopes=scopes)

        # Build the service object
        try:
            service = discovery.build(api_name, api_version, cache_discovery=False, credentials=credentials)
        except Exception as error:
            raise AirflowException('Erro ao conectar Google Drive API') from error

        return service

    def _get_gsheet_modifiedTime(self) -> datetime:
        """
        Retorna data de última alteração de arquivo no Google Drive.

        Return:
            - results (datetime): Datetime da última atualização do arquivo
        """

        # Define the auth scopes to request
        scopes = ['https://www.googleapis.com/auth/drive.metadata.readonly']

        # Authenticate and construct service
        service = self.get_google_service(
                api_name='drive',
                api_version='v3',
                scopes=scopes)

        # Get modifiedTime
        results = service.files().get(
                fileId=self.spreadsheet_id,
                fields='modifiedTime').execute()

        return convert_gsheets_str_to_datetime(results['modifiedTime'])

    def _get_gsheet_api_service(self) -> discovery.Resource:
        """
        Get a service that communicates to the Spreadsheet Google API v4
        having read and write permissions.

        Return:
            - results (discovery.Resource): A service that is connected
              to the Spreadsheet API v4 for read and
            write permissions.
        """

        scopes = 'https://www.googleapis.com/auth/spreadsheets'

        return self.get_google_service(
            api_name='sheets',
            api_version='v4',
            scopes=scopes)

    def get_gsheet_df(self,
        sheet_name: str,
        has_header: bool = True) -> pd.DataFrame:
        """
        Extract data from google spreadsheet and return as a Pandas
        Dataframe.

        Args:
            - sheet_name (str): Name of the specific sheet in the
            spreadsheet.
            - has_header (bool): If use first row to name
            columns. Default= True.

        Return:
            - results (pandas.dataframe): Generated pandas dataframe
            containing all the sheet columns and rows. By default uses
            first line as column names.
        """

        wst = self._get_worksheet(sheet_name=sheet_name)

        df = wst.get_as_df(has_header=has_header)

        if has_header:
            new_header = pd.Series(df.columns)
            df.columns = new_header.apply(slugify_column_names)

        return df

    def _get_worksheet(self, sheet_name: str) -> pygsheets.worksheet:
        """
        Get Worksheet object wrapper of pygsheets lib refering the
        spreadsheet_id and sheet_name specified.

        Args:
            - sheet_name (str): Name of the specific sheet in the
            spreadsheet.

        Return:
            - results (pygsheets.worksheet): Pygsheets worksheet wrapper.
        """

        # Autentica via variável de ambiente
        sa_env_var = "GDRIVE_API_CREDENTIALS"
        os.environ[sa_env_var] = BaseHook.get_connection(self.conn_id).password
        gc = pygsheets.authorize(service_account_env_var=sa_env_var)
        sht = gc.open_by_key(self.spreadsheet_id)
        wst = sht.worksheet('title', sheet_name)

        return wst

    def set_df_to_gsheet(self,
                         df: pd.DataFrame,
                         sheet_name: str,
                         copy_head: bool=True):
        """
        Writes the pandas dataframe content to the specified spreadsheet. Writes
        the dataframe header as first row by default.

        Args:
            - df (pandas.dataframe): Dataframe to be written.
            - sheet_name (str): Name of the specific sheet in the
            spreadsheet.
            - copy_head (bool): Copy header data into first row.
        """

        wst = self._get_worksheet(sheet_name=sheet_name)

        wst.clear()
        wst.set_dataframe(df=df, start='A1', copy_head=copy_head, extend=True)

    def get_sheet_id(self, sheet_name: str) -> int:
        """
        Get the identifier of a specific worksheet (sheet_id) from its name in a
        Google spreadsheet.

        Args:
            - sheet_name (str): Name of the specific sheet in the
            spreadsheet.

        Return:
            - results (int): Google spreadsheet id.
        """
        wst = self._get_worksheet(sheet_name=sheet_name)

        return wst.jsonSheet['properties']['sheetId']

    def check_gsheet_file_update(self, until_date: datetime):
        """
        Pega última atualização do arquivo GoogleSheets e compara com a
        data recebida.

        Args:
            - until_date (datetime): data limite para atualização do arquivo

        Return:
            Booleano da comparação das datas
        """

        update_date = self._get_gsheet_modifiedTime()

        print(f'Última atualização do arquivo em: {update_date}')

        return bool(update_date.date() >= until_date.date())

    def format_sheet(self, sheet_name: str, start: str, end: str, fields: str,
        cell_json:str, model_cell: str = "A1"):
        """
        Altera a formatação do intervalo de células da planilha.

        Args:
            - sheet_name (str): nome da Worksheet
            - start (str): endereço da célula de início de intervalo
                (Ex.: "A1")
            - end (str): endereço da célula de final de intervalo
                (Ex.: "A10")
            - fields (str): lista de nomes de campos para aplicar à
                formatação de célula (Consulte doc. Pygsheets)
            - cell_json (str): JSON de formatação para aplicar à
                célula (Consulte doc. Pygsheets)
            - model_cell (:obj:`str`, optional): célula para ser
                utilizada como modelo de formatação
        """

        cell = pygsheets.Cell(model_cell)

        wst = self._get_worksheet(sheet_name=sheet_name)

        data_range = pygsheets.DataRange(start=start, end=end, worksheet=wst)

        data_range.apply_format(cell=cell, fields=fields,
            cell_json=cell_json)