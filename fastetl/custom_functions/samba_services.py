"""
Módulo para manipulação de dados em servidor de arquivo Samba.

last update: 19/06/2020
guilty: Vitor Bellini
"""

import re
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
import pandas as pd
from smb.SMBConnection import SMBConnection

from airflow.hooks.base import BaseHook

from fastetl.custom_functions.utils.string_formatting import slugify_column_names

class SambaConnection():
    """
    Retorna conexão no servidor de arquivo SMB. Para ser usado com
    contexto with.

    Args:
        - samba_conn_id (str): Id da conexão (Airflow) do servidor de
        arquivo SMB. A conexão deve: 1) Utilizar o 'NetBIOS Name' do servidor
        como host; 2) Conter parâmetro extra "{'domain_name': 'MP'}" para
        autenticação com usuário da rede do ministério; 3) Conter parâmetro
        extra 'service_name'; 4) Utilizar parâmetro extra 'IP' para conexão
        através da VPN.
        Ex.: {
               "domain_name": "MP",
               "service_name": "dadossiconv",
               "IP": "10.209.8.101"
             }

    Return:
        - conn: conexão com o servidor de arquivos
        - service_name: nome do serviço cadastrado no Id da conexão (Airflow)
    """

    def __init__(self, samba_conn_id):
        samba_conn_values = BaseHook.get_connection(samba_conn_id)
        self.user_id = samba_conn_values.login
        self.password = samba_conn_values.password
        self.client_machine_name = 'airflow_server'
        self.server_name = samba_conn_values.host
        self.domain_name = samba_conn_values.extra_dejson.get('domain_name')
        self.service_name = samba_conn_values.extra_dejson.get('service_name')
        self.host_ip = samba_conn_values.extra_dejson.get('IP', '')
        self.port = int(samba_conn_values.port)

    def __enter__(self):
        try:
            self.conn = SMBConnection(
                self.user_id,
                self.password,
                self.client_machine_name,
                self.server_name,
                domain=self.domain_name,
                use_ntlm_v2=True)
            self.conn.connect(self.server_name, self.port)
            return self.conn, self.service_name
        except:
            try:
                self.conn = SMBConnection(
                    self.user_id,
                    self.password,
                    self.client_machine_name,
                    self.server_name,
                    domain=self.domain_name,
                    use_ntlm_v2=True)
                self.conn.connect(self.host_ip, self.port)
                return self.conn, self.service_name
            except:
                raise Exception('Samba connection failed.')

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()


def get_file_last_write_time(filepath: str, samba_conn_id: str):
    """
    Retorna data de última alteração de arquivo no servidor de arquivo
    SMB.

    Args:
        - filepath (str): caminho completo do arquivo no servidor
        - samba_conn_id (str): Id da conexão (Airflow) do servidor de
        arquivo SMB

    Return:
        - results (datetime): Datetime da última atualização do arquivo

    Link (lib pysmb):
        - https://pysmb.readthedocs.io/en/latest/api/smb_SMBConnection.html
        - https://pysmb.readthedocs.io/en/latest/api/smb_SharedFile.html
    """

    with SambaConnection(samba_conn_id) as (conn, service_name):
        file_last_write_time = conn.getAttributes(
            service_name,
            filepath).last_write_time

    t_0 = datetime(1970, 1, 1)

    return t_0 + timedelta(seconds=file_last_write_time)

def get_samba_df(filepath: str,
                 samba_conn_id: str,
                 sheet_name=0,
                 header=0,
                 separator: str = None,
                 encoding: str = None,
                 decimal: str = None):
    """
    Extrai dados de planilha excel ou arquivo csv do servidor smb e
    retorna como Pandas Dataframe.

    Args:
        - filepath (str): caminho completo do arquivo no servidor
        - samba_conn_id (str): Id da conexão (Airflow) do servidor de
        arquivo SMB
        - sheet_name (int, str ou list): nome da aba para leitura de
        arquivo xls
        - header (int ou list[int]): identificação das linhas que serão
        consideradas cabeçalho
        - separator (str): separador utilizado no arquivo csv
        - encoding (str): encoding do arquivo csv
        - decimal (str): separador decimal dos números quando csv

    Return:
        - results (pandas.dataframe): Pandas Dataframe contendo todas as
        linhas e colunas da planilha.
    """

    filetype = filepath.split('.')[-1]

    with SambaConnection(samba_conn_id) as (conn, service_name):
        file_obj = NamedTemporaryFile()
        conn.retrieveFile(service_name, filepath, file_obj)

        if filetype in ['xls', 'xlsx']:
            df = pd.read_excel(file_obj, sheet_name=sheet_name, header=header)
        elif filetype in ['csv']:
            file_obj.seek(0)
            df = pd.read_csv(file_obj,
                             sep=separator,
                             encoding=encoding,
                             decimal=decimal)
        else:
            raise Exception('File format not supported. Only .csv, '\
                            '.xls and .xlsx for the moment. Talk to '\
                            'your Data Enginneer fellow.')

    # drop multiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col) for col in df.columns]

    new_header = pd.Series(df.columns)
    df.columns = new_header.apply(slugify_column_names)

    return df

def create_folder(new_folderpath: str, samba_conn_id: str):
    """
    Cria uma nova pasta em um servidor Samba.

    Args:
        - new_folderpath (str): Caminho completo da pasta para criar
        - samba_conn_id (str): Id da conexão (Airflow) do servidor de
        arquivo SMB

    Return:
        - None
    """

    # separo o folder path com o nome da pasta
    new_folderpath = re.sub('/$', '', new_folderpath)
    folder_path, folder_name = new_folderpath.rsplit("/", 1)

    with SambaConnection(samba_conn_id) as (conn, service_name):
        result = conn.listPath(service_name, folder_path)
        if folder_name in [f.filename for f in result if f.isDirectory]:
            print(f"Folder {new_folderpath} exists.")
        else:
            try:
                conn.createDirectory(service_name, new_folderpath)
            except:
                raise Exception(f"Folder {new_folderpath} creation failed.")

def move_files(filepath_to_move: list,
               samba_conn_id: str,
               delete_source: bool = True):
    """
    Função para mover todos os arquivos da pasta origem para a pasta
    destino.

    Args:
        - filepath_to_move (list): lista de tuplas com origem e
        destino do arquivo. Ex. [('/folder1/file.txt'),('/folder2/file.txt')]
        - samba_conn_id (str): Id da conexão (Airflow) do servidor de
        arquivo SMB
        - delete_source (bool): Escolhe se deleta ou não arquivo na origem

    Return:
        - None
    """

    with SambaConnection(samba_conn_id) as (conn, service_name):
        for filepath in filepath_to_move:
            # read
            try:
                file_obj = NamedTemporaryFile()
                conn.retrieveFile(service_name, filepath[0], file_obj)
                file_obj.seek(0)

                # write
                try:
                    conn.storeFile(service_name, filepath[1], file_obj)

                    # delete
                    if delete_source:
                        try:
                            conn.deleteFiles(service_name, filepath[0])
                        except:
                            raise Exception(f'Error deleting source file on "{filepath[0]}"')

                except:
                    raise Exception(f'Error writing file on "{filepath[1]}"')

            except:
                raise Exception(f'Error reading file "{filepath[0]}"')
