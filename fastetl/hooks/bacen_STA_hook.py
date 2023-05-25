"""
Hook para utilização do STA (API do Bancon Central - Bacen). Geralmente
utilizada para download de arquivos de dados.
Manual do STA:
https://www.bcb.gov.br/content/acessoinformacao/sisbacen_docs/Manual_STA_Web_Services.pdf
"""
from datetime import datetime
import xml.etree.ElementTree as ET
import base64
import string
from random import choice
import requests
import pytz

from airflow import settings
from airflow.models import Connection
from airflow.utils.email import send_email
from airflow.hooks.base import BaseHook

from fastetl.custom_functions.utils.encode_html import replace_to_html_encode

class BacenSTAHook(BaseHook):
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    STA_URL = "https://sta.bcb.gov.br/staws"
    STA_PASSW_URL = 'https://www3.bcb.gov.br/senhaws/senha'

    def __init__(self,
                 conn_id,
                 *args,
                 **kwargs):
        self.conn_id = conn_id

    def _get_auth_headers(self):
        """ Função auxiliar para construir os cabeçalhos para as
        requisições à API. Inclui o cabeçalho de autenticação
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
        """
        Validate max_date and generate if its None. Also localize to
        compare properly.
        """
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

    def _get_id_newest_file(self, sistema, data_min, data_max=None) -> str:
        """
        Filtra os arquivos do STA pelo intervalo de datas e retorna o id
        (protocolo) do arquivo mais recente.
        """
        data_min, data_max = self._get_correct_time_range(data_min, data_max)

        querystring = {
            "dataHoraInicio": data_min.strftime(self.DATE_FORMAT)[:23],
            "dataHoraFim": data_max.strftime(self.DATE_FORMAT)[:23],
            "sistemas": sistema
            }
        url = self.STA_URL + "/arquivos/disponiveis"
        response = requests.request("GET",
                                    url,
                                    headers=self._get_auth_headers(),
                                    params=querystring)

        if response.status_code == 401:
            raise ValueError('Web Service do Bacen rejeitou as '
                             'credenciais de login. Confira o login e '
                             'senha cadastrados.')

        xml_tree = ET.fromstring(response.content)
        extract_date = lambda x: datetime.strptime(x, self.DATE_FORMAT)
        data_id_map = {
            extract_date(node.find('DataHoraDisponibilizacao').text):
                node.find('Protocolo').text
            for node in xml_tree.findall('Arquivo')
        }
        if not data_id_map:
            raise ValueError('Web Service do Bacen (STA) respondeu com '
                            'nenhum resultado.')
        newest_date = max(data_id_map.keys())
        return data_id_map[newest_date]

    def download_latest_file(self,
                             dest_file_path: str,
                             sistema: str,
                             data_min: datetime,
                             data_max: datetime = None):
        """
        Realiza o download do arquivo mais recente. Recebe a janela de
        filtro em dias para reduzir carga na API. Utiliza 30 dias como padrão.
        """
        id_newest_file = self._get_id_newest_file(sistema, data_min, data_max)

        file_url = self.STA_URL + f"/arquivos/{id_newest_file}/conteudo"
        raw_file = requests.request("GET",
                                    file_url,
                                    headers=self._get_auth_headers(),
                                    stream=True)

        with open(dest_file_path, "wb") as file:
            for chunk in raw_file.iter_content(chunk_size=512):
                file.write(chunk)

        print(f"Downloaded file with id {id_newest_file}.")

    def _generate_new_password(self):
        return ("".join(choice(string.ascii_letters) for x in range(2))).upper() + \
               ("".join(choice(string.ascii_letters) for x in range(3))).lower() + \
               "@" + \
               "".join(choice(string.digits) for x in range(3))

    def _send_email_password_updated(self, new_pass, email_to_list):
        """Envia email para admins informando a nova senha após atualização
        """
        conn_values = BaseHook.get_connection(self.conn_id)

        subject = "Atualização da senha da WS do BACEN (STA)"
        dag_id = 'update_password_sta_bacen'
        dag_url = f'http://airflow.seges.mp.intra/tree?dag_id={dag_id}'
        content = f"""
            Olá!
            <br>
            <br>
            A senha utilizada pelas dags que acessam arquivos no BACEN
            via o STA foi atualizada.
            <br>
            <br>
            A nova senha é: <b>{new_pass}</b>
            <br>
            O login é: <b>{conn_values.login}</b>
            <br>
            Caso queira testar acesse:
            <a href="https://sta.bcb.gov.br/"
               target="_blank">https://sta.bcb.gov.br/</a>
            <br>
            <br>
            Esta mensagem foi enviada pela dag <b>{dag_id}</b>.
            <br>
            Para acessar: <a href="{dag_url}" target="_blank">{dag_url}</a>
            <br>
            <br>
            Airflow-bot.
            <br>
            <br>
        """
        send_email(to=email_to_list,
                   subject=subject,
                   html_content=replace_to_html_encode(content))

    def update_password(self, email_to_list):
        """
        Call web service method to update the credential password. Its
        necessary due to expiration rules in the WS.
        """
        # Get the sql alchemy object to persist changes if update succeed
        session = settings.Session()
        conn_model = session\
        .query(Connection)\
        .filter(Connection.conn_id == self.conn_id)\
        .first()

        cur_pass = conn_model.password
        new_pass = self._generate_new_password()

        request_template = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <Parametros>
            <Senha>{cur_pass}</Senha>
            <NovaSenha>{new_pass}</NovaSenha>
            <ConfirmacaoNovaSenha>{new_pass}</ConfirmacaoNovaSenha>
        </Parametros>
        """
        headers = self._get_auth_headers()
        headers['Content-Type'] = 'application/xml'
        response = requests.request('PUT',
                                    self.STA_PASSW_URL,
                                    headers=headers,
                                    data=request_template)

        if response.status_code == 204:
            conn_model.set_password(new_pass)
            session.add(conn_model)
            session.commit()
            self._send_email_password_updated(new_pass, email_to_list)
        else:
            print('response.status_code HTTP: ', response.status_code)
            print(response.content)
            raise Exception('A atualização da senha falhou.')
