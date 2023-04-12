"""Airflow hooks to access the Dados Abertos Gov.br API to create and update resources.
API Documentation: https://dados.gov.br/swagger-ui.html
"""
import requests
import json
import logging
from functools import cached_property
from collections import ChainMap
from airflow.hooks.base import BaseHook
from urllib.parse import urljoin


class DadosGovBrHook(BaseHook):
    """Provides access to the API.
    """

    def __init__(self,
        conn_id: str,
        *args,
        **kwargs
        ):
        self.conn_id = conn_id

    @cached_property
    def api_connection(self) -> str:
        """Gets the API host and token from the Airflow connection.
        """
        conn = BaseHook.get_connection(self.conn_id)
        url = getattr(conn, "host", None)
        token = getattr(conn, "password", None)
        return url, token


    def _get_dataset(self, id: str):
        """Return specified dataset information and resources.
        Endpoint: /dados/api/publico/conjuntos-dados/{id}
        """
        slug = f"/dados/api/publico/conjuntos-dados/{id}"

        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": self.api_connection[1],
        }
        req_url = urljoin(self.api_connection[0], slug)
        response = requests.request(method="GET",
                            url=req_url,
                            headers=headers
                            )
        try:
            response.raise_for_status()
        except Exception as error:
            raise Exception("Erro ao retornar o conjunto de dados na API") \
            from error

        dataset = json.loads(response.text)

        return dataset

    def _get_if_resource_exists(self,
                                  dataset:dict,
                                  url: str):
        matching_resources = [
            resource \
            for resource in dataset["recursos"] \
            if resource["link"] == url]

        return matching_resources[0]

            #update_method

    # def update_dataset(
    #     self,
    #     dataset_id: str,
    #     **properties
    #     ):
    #     "Update some properties of the dataset on CKAN."
    #     catalog = self._get_catalog()
    #     catalog.action.package_patch(id=dataset_id, **properties)

    def create_or_update_resource(
        self,
        dataset_id: str,
        name: str,
        url: str,
        format: str,
        description: str = None,
        type: str = "DADOS",
        ):
        "Creates or updates a resource."
        dataset = self._get_dataset(id=dataset_id)
        existing_resource = self._get_if_resource_exists(dataset=dataset, url=url)

        if existing_resource:
            resource = dict(ChainMap(
                {
                    'titulo': name,
                    'link': url,
                    'descricao': resource['descricao'] if description is None else description,
                    'formato': format,
                },
                existing_resource
            ))
        else: # create resource
            resource = {
                'idConjuntoDados': dataset_id,
                'titulo': name,
                'link': url,
                'descricao': description,
                'tipo': 'DADOS',
                'formato': format,
            }

        print (resource)

        slug = "recurso/salvar"

        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": self.api_connection[1],
        }

        req_url = urljoin(self.api_connection[1], slug)

        response = requests.request(method="POST",
                                    url=req_url,
                                    headers=headers,
                                    json=resource,
                                    )

        response = json.loads(response.text)

        try:
            response.raise_for_status()
            if existing_resource:
                logging.info("Recurso atualizado com sucesso")
            else:
                logging.info("Novo recurso inserido com sucesso")
        except Exception as error:
            raise Exception("Erro ao salvar o recurso") \
            from error