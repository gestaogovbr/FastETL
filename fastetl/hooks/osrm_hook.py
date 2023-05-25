"""Airflow hooks to access the Open Street Routing Machine (OSRM) API to
calculate routes and distances.
"""
import urllib
from functools import cached_property
from typing import Tuple

import requests

from airflow.hooks.base import BaseHook

from fastetl.custom_functions.config import USER_AGENT

class OSRMHook(BaseHook):
    """Provides access to the Open Street Routing Machine (OSRM) API.

    Args:
        conn_id (str): The Airflow connection id to use.

    Attributes:
        api_endpoint (str): URL of the OSRM API endpoint.

    Examples:
        Instantiate the class by passing the Airflow connection id.

        >>> hook = OSRMHook(conn_id='osrm_api')
        >>> isinstance(hook, OSRMHook)
        True
        >>> route = hook.get_route(
                origin=(-15.799114,-47.871450),
                destination=(-15.870442,-47.921462)
            )
        >>> distance = get_shortest_distance(route)
        >>> print(distance)
        15.4438
    """

    def __init__(self,
        conn_id: str,
        *args,
        **kwargs
        ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @cached_property
    def api_endpoint(self) -> str:
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
        profile: str = 'driving',
        steps: bool = True) -> dict:
        """Gets the calculated routes from the OSRM API from the defined
        origin and destination point, using the specified profile.

        Args:
            origin (float, float): The point of origin for the
                route, as a tuple of latitude, longitude.
            destination (float, float): The point of destination
                for the route, as a tuple of latitude, longitude.
            profile (:obj:`str`, optional): The profile for calculating
                the route (e.g.) 'bike' or 'foot'. Defaults to 'driving'.
            steps (bool): Include a step by step description of the
                route.

        Returns:
            dict: A `dict` with the data returned from the call to the
                OSRM API.

        Raises:
            ValueError: If the code returned by the API is not 200.
        """
        lat_o, long_o = origin
        lat_d, long_d = destination
        url = urllib.parse.urljoin(
            self.api_endpoint,
            f'/route/v1/{profile}/{long_o},{lat_o};{long_d},{lat_d}'
        )

        response = requests.get(
            url,
            params={'steps': str(steps).lower()},
            headers={'User-Agent': USER_AGENT}
        )

        if response.status_code != requests.codes.ok:
            raise ValueError(f'OSRM API returned code {response.status_code}.')

        return response.json()

def get_shortest_distance(data: dict) -> float:
    """Gets the distance of the shortest route using the OSRM API.

    Args:
        data (dict): The route data returned by `get_route`.

    Returns:
        float: The distance, in kilometers, of the first (shortest)
            route.
    """
    if data['code'] == 'Ok':
        if (
            len(data['routes']) > 0  # has at least one route
            and data['routes'][0].get('geometry', None) is not None
            and data['routes'][0]['distance'] > 0
        ):
            return data['routes'][0]['distance'] / 1000.0
    return None
