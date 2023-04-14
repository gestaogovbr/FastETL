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
from typing import Union


class DadosGovBrHook(BaseHook):
    """
    Provides access to the Dados Abertos Gov.br API and datasets resources
    """

    def __init__(self,
        conn_id: str,
        *args,
        **kwargs
        ):
        self.conn_id = conn_id

    @cached_property
    def api_connection(self) -> tuple:
        """
        Retrieve the API connection details from the Airflow connection.

        Returns:
            tuple: A tuple containing the API URL and token.
        """

        conn = BaseHook.get_connection(self.conn_id)
        url = getattr(conn, "host", None)
        token = getattr(conn, "password", None)
        return url, token


    def _get_dataset(self, id: str) -> dict:
        """
        Retrieve a dataset from the API by its ID.
        Endpoint: /dados/api/publico/conjuntos-dados/{id}

        Args:
            id (str): A string representing the ID of the dataset.

        Returns:
            dict: A dictionary containing the metadata and resources of
            the retrieved dataset.

        Raises:
            Exception: If an error occurs while making the API request
            or processing the response.
        """

        slug = f"/dados/api/publico/conjuntos-dados/{id}"

        api_url, token = self.api_connection

        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": token,
        }
        req_url = urljoin(api_url, slug)
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
                                  url: str) -> Union[dict, bool]:
        """ Check if a resource exists in a dataset by matching its URL.

        Args:
            dataset (dict): dataset dictionary as returned by the API
            url (str): The URL file of the resource

        Returns:
            dict or bool: If a matching resource is found in the dataset,
            return its dictionary representation. Otherwise, return False.
        """
        matching_resources = [
            resource \
            for resource in dataset["recursos"] \
            if resource["link"] == url]

        return (matching_resources[0] if matching_resources else False)

    # def update_dataset(
    #     self,
    #     dataset_id: str,
    #     title: str,
    #     description: str,
    #     version: float, #confirmar se é float,
    #     ):
    #     "Update some properties of the dataset"

    #     dataset = {
    #         'idConjuntoDados': dataset_id,
    #         'titulo': title,
    #         'descricao': description,
    #         'versao': version,
    #         'temas': []
    #     }

    #     logging.info("Payload: " + str(dataset))

    #     slug = f"public/conjunto-dados/{dataset_id}"
    #     api_url, token = self.api_connection
    #     headers = {
    #         "accept": "application/json",
    #         "chave-api-dados-abertos": token,
    #     }

    #     req_url = urljoin(api_url, slug)

    #     response = requests.request(method="PATCH",
    #                                 url=req_url,
    #                                 headers=headers,
    #                                 json=dataset,
    #                                 )

    #     try:
    #         response.raise_for_status()
    #         logging.info("Conjunto de Dados atualizado com sucesso")

    #     except Exception as error:
    #         raise Exception("Erro ao atualizar o Dataset") \
    #         from error


    def create_or_update_resource(
        self,
        dataset_id: str,
        name: str,
        url: str,
        format: str,
        description: str = None,
        type: str = "DADOS",
        ):
        """
        Create or update a resource for a given dataset.

        Example:
            create_or_update_resource(
                dataset_id="3b8b981c-3e44-4df2-a9f6-2473ee4caf83",
                name="SIORG - Distribuição de Cargos e Funções para o
                mês de março/2023",
                url="https://repositorio.dados.gov.br/seges/siorg/distribuicao/distribuicao-orgaos-siorg-2023-03.zip",
                format="ZIP",
                description="Contém a distribuição dos cargos e funções
                    ao longo da estrutura organizacional dos órgãos e entidades
                    que fazem parte do SIORG, para o mês de março/2023",
                    type="DADOS",
            )

        Args:
            dataset_id (str): A string representing the ID of the dataset
                to create or update the resource for.
            name (str): A string representing the title of the resource.
            url (str): A string representing the URL link of the resource.
            format (str): A string representing the format of the file.
            description (str, optional): An optional string representing
                the description of the resource. Defaults to None.
            type (str, optional): An optional string representing the
                type of the resource. Defaults to "DADOS". Valid options:
                [INVALIDO, DADOS, DOCUMENTACAO, DICIONARIO_DE_DADOS, API, OUTRO]


        Returns:
            None

        Raises:
            Exception: If an error occurs while creating or updating the
                resource.
        """

        dataset = self._get_dataset(id=dataset_id)
        existing_resource = self._get_if_resource_exists(dataset=dataset,
                                                         url=url)

        if existing_resource:
            resource = dict(ChainMap(
                {
                    'titulo': name,
                    'link': url,
                    'descricao': resource['descricao'] \
                          if description is None else description,
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
                'tipo': type,
                'formato': format,
            }

        logging.info("Payload: " + str(resource))

        slug = "recurso/salvar"
        api_url, token = self.api_connection
        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": token,
        }

        req_url = urljoin(api_url, slug)

        response = requests.request(method="POST",
                                    url=req_url,
                                    headers=headers,
                                    json=resource,
                                    )

        try:
            response.raise_for_status()
            if existing_resource:
                logging.info("Recurso atualizado com sucesso")
            else:
                logging.info("Novo recurso inserido com sucesso")
        except Exception as error:
            raise Exception("Erro ao salvar o recurso") \
            from error
