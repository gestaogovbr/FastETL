"""Airflow hooks to access the OSRM API to calculate routes and
distances.
"""
import urllib
import requests
from functools import cached_property
from typing import Tuple

from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook

from FastETL.custom_functions.config import USER_AGENT

class OSRMHook(BaseHook):
    """Provides access to the Open Street Routing Machine (OSRM) API.
    """

    @apply_defaults
    def __init__(self,
        conn_id: str,
        *args,
        **kwargs
        ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @cached_property
    def api_endpoint(self):
        """Gets the API endpoint from the Airflow connection.
        """
        conn = BaseHook.get_connection(self.conn_id)
        osrm_url = f"{conn.schema}://{conn.host}"
        if getattr(conn, "port", None):
            osrm_url += f":{conn.port}"
        return osrm_url

    def get_route(self,
        origin: Tuple[float, float],
        destination: Tuple[float, float],
        profile: str = 'driving') -> dict:
        """Gets the calculated routes from the OSRM API from the defined
        origin and destination point, using the specified profile.
        """
        lat_o, long_o = origin
        lat_d, long_d = destination
        url = urllib.parse.urljoin(
            self.api_endpoint,
            f'/route/v1/{profile}/{long_o},{lat_o};{long_d},{lat_d}'
        )

        response = requests.get(
            url,
            params={'steps': 'true'},
            headers={'User-Agent': USER_AGENT}
        )

        if response.status_code != requests.codes.ok:
            raise ValueError(f'OSRM API returned code {response.status_code}.')

        return response.json()

    @staticmethod
    def get_shortest_distance(data: dict) -> float:
        """Gets the distance of the shortest route using the OSRM API.
        """
        if data['code'] == 'Ok':
            return data['routes'][0]['distance'] / 1000.0
        return None
