"""
Hook para realizar operações de consultas à API do Diário Oficial da União.
"""
from datetime import datetime, timedelta
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

class Field(Enum):
    """Define the search field options to be used as parameter in the search
    """
    TUDO = 'tudo'
    TITULO = 'title_pt_BR'
    CONTEUDO = 'ddm__text__21040__texto_pt_BR'

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

    def _get_query_str(self, term, field, is_exact_search):
        """
        Adiciona aspas duplas no inicio e no fim de cada termo para o
        caso de eles serem formados por mais de uma palavra
        """
        if is_exact_search:
            term = f'"{term}"'

        if field == Field.TUDO:
            return term
        else:
            return f'{field.value}-{term}'

    def calculate_from_datetime(self,
                                publish_to_date: datetime,
                                search_date: SearchDate):
        """
        Calculate parameter `publishFrom` to be passed to the API based
        on publishTo parameter and `search_date`. Perform especial
        calculation to the MES (month) parameter option
        """
        if search_date == SearchDate.DIA:
            return (publish_to_date - timedelta(days=1))

        elif search_date == SearchDate.SEMANA:
            return (publish_to_date - timedelta(days=7))

        elif search_date == SearchDate.MES:
            end_last_month = publish_to_date.replace(day=1) - timedelta(days=1)
            publish_from_date = end_last_month.replace(day=publish_to_date.day)
            return publish_from_date

        elif search_date == SearchDate.ANO:
            return (publish_to_date - timedelta(days=365))


    def search_text(self, search_term: str,
                          sections: [Section],
                          reference_date:datetime=datetime.now(),
                          search_date=SearchDate.DIA,
                          field=Field.TUDO,
                          is_exact_search=True):
        """
        Search for a term in the API and return all ocurrences.

        Args:
            - search_term: The term to perform the search with.
            - section: The Journal section to perform the search on.

        Return:
            - A list of dicts of structred results.
        """

        publish_from = self.calculate_from_datetime(reference_date, search_date)

        payload = [
            ('q', self._get_query_str(search_term, field, is_exact_search)),
            ('exactDate', 'personalizado'),
            ('publishFrom', publish_from.strftime('%d-%m-%Y')),
            ('publishTo', reference_date.strftime('%d-%m-%Y')),
            ('sortType', '0')
        ]
        for section in sections:
            payload.append(('s', section.value))

        page = requests.get(self.IN_API_BASE_URL, params=payload)
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
                item['section'] = content['pubName'].lower()
                item['title'] = content['title']
                item['href'] = self.IN_WEB_BASE_URL + content['urlTitle']
                item['abstract'] = content['content']
                item['date'] = content['pubDate']

                all_results.append(item)

        return all_results
