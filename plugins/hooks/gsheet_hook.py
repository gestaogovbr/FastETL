"""
Hook customizado para realizar operações de leitura e escrita com
planilhas do Google através de sua API (google sheets).
"""

import json
import os
import pandas as pd

from airflow import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

from oauth2client.service_account import ServiceAccountCredentials
from apiclient import discovery
from httplib2 import Http
import pygsheets

from custom_functions.utils.string_formatting import slugify_column_names
from custom_functions.utils.string_formatting import convert_gsheets_str_to_datetime

class GSheetHook(BaseHook):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 spreadsheet_id,
                  *args,
                  **kwargs):
        self.conn_id = conn_id
        self.spreadsheet_id = spreadsheet_id

    def get_google_service(self, api_name, api_version, scopes):
        """
        Get a service that communicates to the Google API.

        Args:
            - api_name: The name of the api to connect to.
            - api_version: The api version to connect to.
            - scopes: A list auth scopes to authorize for the application.

        Return:
            - A service that is connected to the specified API.
        """

        # Transform key_id to json
        key_str = BaseHook.get_connection(self.conn_id).password
        key_value = json.loads(key_str)

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(key_value, scopes=scopes)

        # Build the service object
        try:
            # service = build(api_name, api_version, credentials=credentials)
            http_auth = credentials.authorize(Http())
            service = discovery.build(api_name, api_version, http=http_auth)
        except:
            raise AirflowException('Erro ao conectar Google Drive API')

        return service

    def _get_gsheet_modifiedTime(self):
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

    def _get_gsheet_api_service(self):
        """
        Get a service that communicates to the Spreadsheet Google API v4 having
        read and write permissions.

        Return:
            - A service that is connected to the Spreadsheet API v4 for read and
            write permissions.
        """

        scopes = 'https://www.googleapis.com/auth/spreadsheets'

        return self.get_google_service(
            api_name='sheets',
            api_version='v4',
            scopes=scopes)

    def get_gsheet_df(self, sheet_name: str, first_row_as_header: bool=True):
        """
        Extract data from google spreadsheet and return as a Pandas
        Dataframe.

        Args:
            - sheet_name (str): Name of the specific sheet in the
            spreadsheet.
            - first_row_as_header (str): If use first row to name
            columns. Default= True.

        Return:
            - results (pandas.dataframe): Generated pandas dataframe
            containing all the sheet columns and rows. By default uses
            first line as column names.
        """

        # Authenticate and construct service
        service = self._get_gsheet_api_service()

        sheets = service.spreadsheets()
        result = sheets.values().get(spreadsheetId=self.spreadsheet_id,
                                        range=sheet_name).execute()
        data_values = result.get('values', [])
        df = pd.DataFrame(data_values)

        if first_row_as_header:
            new_header = df.iloc[0]
            df = df[1:]
        else:
            new_header = pd.Series(df.columns)

        df.columns = new_header.apply(slugify_column_names)

        return df

    def _get_worksheet(self, sheet_name: str):
        """
        Get Worksheet object wrapper of pygsheets lib refering the spreadsheet_id
        and sheet_name specifieds.

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

    def get_sheet_id(self, sheet_name: str):
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

    def check_gsheet_file_update(self, until_date):
        """
        Pega última atualização do arquivo GoogleSheets e compara com a
        data recebida.

        Args:
            - until_date (date): data limite para atualização do arquivo

        Return:
            Booleano da comparação das datas
        """

        update_date = self._get_gsheet_modifiedTime()

        print(f'Última atualização do arquivo em: {update_date}')

        if update_date.date() >= until_date:
            return True
        else:
            return False

    def batchUpdate(self, updates: str):
        """
        Realiza alterações em lote na planilha seguindo padrão do próprio.

        Args:
            - updates (str): string json contendo as alterações a serem
            executadas na planilha seguindo o formato XXXXXX TODO: completar aqui
        """
        service = self._get_gsheet_api_service()
        service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=updates).execute()