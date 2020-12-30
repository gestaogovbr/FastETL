"""
Hook para utilização do STA (API do Bancon Central - Bacen). Geralmente
utilizada para download de arquivos de dados.
"""
from datetime import datetime
import xml.etree.ElementTree as ET
import base64
import requests
import pytz

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

        message = f"{conn_values.login}:{conn_values.password}"
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')

        headers = {
            "user-agent": "airflow-SEGES-ME",
            "authorization": f"Basic {base64_message}"
            }
        return headers

    def _get_correct_time_range(self, date_min, date_max=None):
        tz = pytz.timezone("America/Sao_Paulo")
        if date_max is None:
            date_max = datetime.now(tz)
        else:
            if not date_max.tzinfo:
                date_max = pytz.utc.localize(date_max, is_dst=None).astimezone(tz)
            # Regra do webservice do BACEN
            if date_max > datetime.now(tz):
                raise Exception('data_max não pode ser maior que ' \
                                'data atual. É necessário considerar ' \
                                'o timezone do Airflow.')
        if not date_min.tzinfo:
            date_min = pytz.utc.localize(date_min, is_dst=None).astimezone(tz)

        return date_min, date_max

    def _get_id_newest_file(self, data_min, data_max=None) -> str:
        """
        Filtra os arquivos do STA pelo intervalo de datas e retorna o id
        (protocolo) do arquivo mais recente.
        """
        data_min, data_max = self._get_correct_time_range(data_min, data_max)

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
                             data_min: datetime,
                             data_max: datetime = None):
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
