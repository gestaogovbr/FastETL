"""Airflow hooks to access the Dados Abertos Gov.br API to create and update resources.
API Documentation: https://dados.gov.br/swagger-ui.html
"""
import requests
import json
from collections import ChainMap
from airflow.hooks.base import BaseHook
from urllib.parse import urljoin


class DadosAbertosHook(BaseHook):
    """Provides access to the API.
    """

    def __init__(self,
        conn_id: str,
        *args,
        **kwargs
        ):
        self.conn_id = conn_id

    def _get_dataset(self, id: str):
        """List the resources from an avaliable Dataset.
        Endpoint: /dados/api/publico/conjuntos-dados/{id}
        """
        conn = BaseHook.get_connection(self.conn_id)
        url = getattr(conn, "host", None)
        token = getattr(conn, "password", None)
        endpoint = "/dados/api/publico/conjuntos-dados/"
        payload = {
            "chave-api-dados-abertos": token,
            "id": id,
        }
        req_headers = {'Content-Type': 'application/json'}
        req_url = urljoin(url, endpoint)

        response = requests.request(method="GET",
                                    url=req_url,
                                    headers=req_headers,
                                    json=payload,
                                    verify=False)
        dataset = json.loads(response.text)

        return dataset


        ckan_url = f"{conn.schema}://{conn.host}"
        if getattr(conn, "port", None):
            ckan_url += f":{conn.port}"
        if getattr(conn, "password", None):
            return RemoteCKAN(ckan_url, apikey=conn.password,
                        user_agent=USER_AGENT)
        else:
            return RemoteCKAN(ckan_url,
                        user_agent=USER_AGENT)

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
        catalog = self._get_dataset(id=dataset_id)
        matching_resources = [
            resource \
            for resource in dataset["resources"] \
            if resource["url"] == url]
        if matching_resources:
            resource = matching_resources[0]
            new_resource = dict(ChainMap(
                {
                    'name': name,
                    'url': url,
                    'description': resource['description'] if description is None else description,
                    'format': format
                },
                resource
            ))
            catalog.action.resource_update(**new_resource)
        else: # create resource
            catalog.action.resource_create(
                package_id=dataset_id,
                url=url,
                name=name,
                format=format,
                description=description
            )
