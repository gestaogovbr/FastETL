"""
Hook customizado para realizar operações de leitura e escrita com
planilhas do Google através de sua API (google sheets).
"""

import pandas as pd

from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

from custom_functions.gsheet_services import get_gsheet_api_service
from custom_functions.utils.string_formatting import slugify_column_names

class GSheetHook(BaseHook):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 spreadsheet_id,
                 sheet_name,
                  *args,
                  **kwargs):
        self.conn_id = conn_id
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name

    def get_gsheet_df(self, first_row_as_header: bool=True):
        """
        Extract data from google spreadsheet and return as a Pandas Dataframe.

        Args:
            - conn_id: Airflow Conn Id with JSON Google authentication.
            - spreadsheet_id (str): Google spreadsheet id. Stays in the URL:
            https://docs.google.com/spreadsheets/d/<SPREADSHEETID>/...
            - sheet_name (str): Name of the specific sheet in the spreadsheet.

        Return:
            - results (pandas.dataframe): Generated pandas dataframe containing
            all the sheet columns and rows. By default uses first line as
            column names.
        """

        # Authenticate and construct service
        service = get_gsheet_api_service(conn_id=self.conn_id)

        sheets = service.spreadsheets()
        result = sheets.values().get(spreadsheetId=self.spreadsheet_id,
                                        range=self.sheet_name).execute()
        data_values = result.get('values', [])
        df = pd.DataFrame(data_values)

        if first_row_as_header:
            new_header = df.iloc[0]
            df = df[1:]
        else:
            new_header = pd.Series(df.columns)

        df.columns = new_header.apply(slugify_column_names)

        return df