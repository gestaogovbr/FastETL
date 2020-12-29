"""
Hook para utilização do STA (API do Bancon Central - Bacen). Geralmente
utilizada para download de arquivos de dados.
"""
from datetime import datetime
import xml.etree.ElementTree as ET
import pytz
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

    def _get_id_newest_file(self, data_min, data_max=None) -> str:
        """
        Filtra os arquivos do STA pelo intervalo de datas e retorna o id
        (protocolo) do arquivo mais recente.
        """

        tz = pytz.timezone("America/Sao_Paulo")
        if data_max is None:
            data_max = datetime.now(tz).date()
        elif data_max > datetime.now(tz).date():
            raise Exception('data_max não pode ser maior que data atual. ' \
                            'É necessário considerar o timezone do Airflow.')

        querystring = {
            "dataHoraInicio": data_min.strftime(self.DATE_FORMAT)[:23],
            "dataHoraFim": data_max.strftime(self.DATE_FORMAT)[:23],
            "sistemas": self.sistema
            }
        url = self.STA_URL + "/arquivos/disponiveis"
        response = requests.request("GET",
                                    url,
                                    headers=self._get_request_headers(),
                                    params=querystring)
        # TODO: Tratar erro de login/senha com mensagem esclarecedora

        xml_tree = ET.fromstring(response.content)

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
                             data_min,
                             data_max=None):
        """
        Realiza o download do arquivo mais recente. Recebe a janela de
        filtro em dias para reduzir carga na API. Utiliza 30 dias como padrão.
        """
        id_newest_file = self._get_id_newest_file(data_min, data_max)

        file_url = self.STA_URL + f"/arquivos/{id_newest_file}/conteudo"
        raw_file = requests.request("GET",
                                    file_url,
                                    headers=self._get_request_headers(),
                                    stream=True)

        with open(dest_file_path, "wb") as file:
            for chunk in raw_file.iter_content(chunk_size=512):
                file.write(chunk)

        print(f"Downloaded file with id {id_newest_file}.")
