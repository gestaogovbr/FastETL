"""
Hook para utilização do STA (API do Bancon Central - Bacen). Geralmente
utilizada para download de arquivos de dados.
"""
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import requests

from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

class BacenSTAHook(BaseHook):
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    STA_URL = "https://sta.bcb.gov.br/staws"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sistema,
                  *args,
                  **kwargs):
        self.conn_id = conn_id
        self.sistema = sistema

    def _get_request_headers(self):
        """ Função auxiliar para construir os cabeçalhos para requisições HTTP
        """
        conn_values = BaseHook.get_connection(self.conn_id)
        headers = {
            "user-agent": "airflow-SEGES-ME",
            "authorization": conn_values.extra_dejson.get("authorization")
            }
        return headers

    def _get_id_newest_file(self, lastdays_filter: int) -> str:
        """
        Filtra os arquivos do STA pela data e retorna o id (protocolo) do
        arquivo mais recente.
        """
        data_inicio = (datetime.now() - timedelta(days=lastdays_filter))
        querystring = {
            "dataHoraInicio": data_inicio.strftime(self.DATE_FORMAT)[:23],
            "sistemas": self.sistema
            }
        url = self.STA_URL + "/arquivos/disponiveis"
        list_xml = requests.request("GET",
                                    url,
                                    headers=self._get_request_headers(),
                                    params=querystring)

        xml_tree = ET.fromstring(list_xml.content)

        extract_date = lambda x: datetime.strptime(x, self.DATE_FORMAT)
        data_id_map = {
                extract_date(node.find('DataHoraDisponibilizacao').text):
                node.find('Protocolo').text
            for node in xml_tree.findall('Arquivo')
        }
        newest_date = max(data_id_map.keys())
        return data_id_map[newest_date]

    def download_latest_file(self,
                             dest_file_path: str,
                             lastdays_filter: int=30):
        """
        Realiza o download do arquivo mais recente. Recebe a janela de
        filtro em dias para reduzir carga na API. Utiliza 30 dias como padrão.
        """
        id_newest_file = self._get_id_newest_file(lastdays_filter)

        file_url = self.STA_URL + f"/arquivos/{id_newest_file}/conteudo"
        raw_file = requests.request("GET",
                                    file_url,
                                    headers=self._get_request_headers(),
                                    stream=True)

        with open(dest_file_path, "wb") as file:
            for chunk in raw_file.iter_content(chunk_size=512):
                file.write(chunk)

        print(f"Downloaded file com id {id_newest_file}.")
