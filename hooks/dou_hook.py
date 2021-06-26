"""
Hook para realizar operações de consultas à API do Diário Oficial da União.
"""
from enum import Enum
import json
import requests

from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

from bs4 import BeautifulSoup

class Section(Enum):
    """Define the section options to be used as parameter in the search
    """
    SECAO_1 = 'do1'
    SECAO_2 = 'do2'
    SECAO_3 = 'do3'
    EDICAO_EXTRA = 'doe'
    EDICAO_SUPLEMENTAR = 'do1a'
    TODOS = 'todos'

class SearchDate(Enum):
    """Define the search date options to be used as parameter in the search
    """
    DIA = 'dia'
    SEMANA = 'semana'
    MES = 'mes'
    ANO = 'ano'

class DOUHook(BaseHook):
    IN_WEB_BASE_URL = 'https://www.in.gov.br/web/dou/-/'
    IN_API_BASE_URL = 'https://www.in.gov.br/consulta/-/buscar/dou'
    SEC_DESCRIPTION = {
        Section.SECAO_1.value: 'Seção 1',
        Section.SECAO_2.value: 'Seção 2',
        Section.SECAO_3.value: 'Seção 3',
        Section.EDICAO_EXTRA.value: 'Edição Extra',
        Section.EDICAO_SUPLEMENTAR.value: 'Edição Suplementar',
        Section.TODOS.value: 'Todas'
    }

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        pass

    def search_text(self, search_term: str,
                          sections: [Section],
                          exact_date=SearchDate.DIA):
        """
        Search for a term in the API and return all ocurrences.

        Args:
            - search_term: The term to perform the search with.
            - section: The Journal section to perform the search on.

        Return:
            - A list of dicts of structred results.
        """

        # Adiciona aspas duplas no inicio e no fim de cada termo para o
        # caso de eles serem formados por mais de uma palavra
        payload = [
            ('q', f'"{search_term}"'),
            ('exactDate', exact_date.value),
            ('sortType', '0')
        ]
        for section in sections:
            payload.append(('s', section.value))

        page = requests.get(self.IN_API_BASE_URL, params=payload, verify=False)
        soup = BeautifulSoup(page.content, 'html.parser')

        script_tag = soup.find(
            'script',
            id='_br_com_seatecnologia_in_buscadou_BuscaDouPortlet_params'
        )
        search_results = json.loads(script_tag.contents[0])['jsonArray']
        all_results = []
        if search_results:
            for content in search_results:
                item = {}
                item['section'] = str(sections)
                item['title'] = content['title']
                item['href'] = self.IN_WEB_BASE_URL + content['urlTitle']
                item['abstract'] = content['content']
                item['date'] = content['pubDate']

                all_results.append(item)

        return all_results
