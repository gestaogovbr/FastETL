"""Airflow hooks to access the dados.gov.br API to create and update resources.

API Documentation: https://dados.gov.br/swagger-ui.html
Caveat: The API documentation is out of date in regards to the current way
the API operates.
"""

from collections import ChainMap
from functools import cached_property
import logging
from typing import Union
from urllib.parse import urljoin

import requests

from airflow.hooks.base import BaseHook

REQUEST_TIMEOUT = 180


class DadosGovBrHook(BaseHook):
    """
    Provides access to the dados.gov.br API and datasets resources.
    """

    def __init__(
        self, conn_id: str, *args, request_timeout: int = REQUEST_TIMEOUT, **kwargs
    ):
        """Instantiate the DadosGovBrHook.

        Args:
            conn_id (str): Airflow connection id.
            request_timeout (int, optional): Maximum time to wait for a
                request, in seconds. Defaults to 180.
        """
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.request_timeout = request_timeout

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

    def _get_dataset(self, dataset_id: str) -> dict:
        """
        Retrieve a dataset from the API by its ID.
        Endpoint: /api/publico/conjuntos-dados/{id}

        Args:
            dataset_id (str): A string representing the ID of the dataset.

        Returns:
            dict: A dictionary containing the metadata and resources of
            the retrieved dataset.

        Raises:
            Exception: If an error occurs while making the API request
            or processing the response.
        """

        slug = f"/dados/api/publico/conjuntos-dados/{dataset_id}"

        api_url, token = self.api_connection

        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": token,
        }
        req_url = urljoin(api_url, slug)
        response = requests.get(
            url=req_url, headers=headers, timeout=self.request_timeout
        )
        response.raise_for_status()

        dataset = response.json()

        return dataset

    def _get_if_resource_exists(self, dataset: dict, link: str) -> Union[dict, bool]:
        """Check if a resource exists in a dataset by matching its URL.

        Args:
            dataset (dict): dataset dictionary as returned by the API
            link (str): The URL file of the resource

        Returns:
            dict or bool: If a matching resource is found in the dataset,
            return its dictionary representation. Otherwise, return False.
        """
        matching_resources = [
            resource for resource in dataset["recursos"] if resource["link"] == link
        ]

        return matching_resources[0] if matching_resources else False

    def update_dataset(self, dataset_id: str, **properties):
        """Update some properties of a given dataset
            Endpoint: /dados/api/publico/conjuntos-dados/{id}

        Args:
            dataset_id (str): The ID of the dataset to be updated.
            **properties: Keyword arguments representing the properties to be updated.

        Raises:
            requests.exceptions.HTTPError: If the API returns an HTTP error status.
            Exception: If an error occurs during the dataset update process.
        """

        logging.info("Payload: %s: %s", dataset_id, properties)

        slug = f"publico/conjuntos-dados/{dataset_id}"

        api_url, token = self.api_connection

        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": token,
        }

        req_url = urljoin(api_url, slug)

        response = requests.patch(
            url=req_url,
            headers=headers,
            json=properties,
            timeout=self.request_timeout,
        )

        response.raise_for_status()
        logging.info("Conjunto de Dados atualizado com sucesso.")

    def create_or_update_resource(
        self,
        dataset_id: str,
        titulo: str,
        link: str,
        formato: str,
        descricao: str = None,
        tipo: str = "DADOS",
    ):
        """
        Create or update a resource for a given dataset.

        Example:
            create_or_update_resource(
                dataset_id="3b8b981c-3e44-4df2-a9f6-2473ee4caf83",
                titulo="SIORG - Distribuição de Cargos e Funções para o
                mês de março/2023",
                link="https://repositorio.dados.gov.br/seges/siorg/distribuicao/distribuicao-orgaos-siorg-2023-03.zip",
                formato="ZIP",
                descricao="Contém a distribuição dos cargos e funções
                    ao longo da estrutura organizacional dos órgãos e entidades
                    que fazem parte do SIORG, para o mês de março/2023",
                    tipo="DADOS",
            )

        Args:
            dataset_id (str): A string representing the ID of the dataset
                to create or update the resource for.
            titulo (str): A string representing the title of the resource.
            link (str): A string representing the URL link of the resource.
            formato (str): A string representing the format of the file.
            descricao (str, optional): An optional string representing
                the description of the resource. Defaults to None.
            tipo (str, optional): An optional string representing the
                type of the resource. Defaults to "DADOS". Valid options:
                [INVALIDO, DADOS, DOCUMENTACAO, DICIONARIO_DE_DADOS, API, OUTRO]


        Returns:
            None

        Raises:
            Exception: If an error occurs while creating or updating the
                resource.
        """

        dataset = self._get_dataset(dataset_id=dataset_id)
        existing_resource = self._get_if_resource_exists(dataset=dataset, link=link)

        if existing_resource:
            resource = dict(
                ChainMap(
                    {
                        "titulo": titulo,
                        "link": link,
                        "descricao": (
                            resource["descricao"] if descricao is None else descricao
                        ),
                        "formato": formato,
                    },
                    existing_resource,
                )
            )
        else:  # create resource
            resource = {
                "idConjuntoDados": dataset_id,
                "titulo": titulo,
                "link": link,
                "descricao": descricao,
                "tipo": tipo,
                "formato": formato,
            }

        logging.info("Payload: %s", resource)

        slug = "recurso/salvar"
        api_url, token = self.api_connection
        headers = {
            "accept": "application/json",
            "chave-api-dados-abertos": token,
        }

        req_url = urljoin(api_url, slug)

        response = requests.post(
            url=req_url,
            headers=headers,
            json=resource,
            timeout=self.request_timeout,
        )

        response.raise_for_status()
        if existing_resource:
            logging.info("Recurso atualizado com sucesso")
        else:
            logging.info("Novo recurso inserido com sucesso")
