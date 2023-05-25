"""Airflow hooks to access the CKAN API to update datasets and resources.
"""

from collections import ChainMap
from ckanapi import RemoteCKAN

from airflow.hooks.base import BaseHook

from fastetl.custom_functions.config import USER_AGENT

class CKANHook(BaseHook):
    """Provides access to the CKAN API.
    """

    def __init__(self,
        conn_id: str,
        *args,
        **kwargs
        ):
        self.conn_id = conn_id

    def _get_catalog(self):
        """Returns an instance of RemoteCKAN that can be used to operate
        the CKAN API.
        """
        conn = BaseHook.get_connection(self.conn_id)
        ckan_url = f"{conn.schema}://{conn.host}"
        if getattr(conn, "port", None):
            ckan_url += f":{conn.port}"
        if getattr(conn, "password", None):
            return RemoteCKAN(ckan_url, apikey=conn.password,
                        user_agent=USER_AGENT)
        else:
            return RemoteCKAN(ckan_url,
                        user_agent=USER_AGENT)

    def update_dataset(
        self,
        dataset_id: str,
        **properties
        ):
        "Update some properties of the dataset on CKAN."
        catalog = self._get_catalog()
        catalog.action.package_patch(id=dataset_id, **properties)

    def create_or_update_resource(
        self,
        dataset_id: str,
        name: str,
        url: str,
        format: str,
        description: str = None,
        ):
        "Creates or updates a resource on CKAN."
        catalog = self._get_catalog()
        dataset = catalog.action.package_show(id=dataset_id)
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
