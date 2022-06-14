"""Airflow operators to calculate distances with the Open Street Routing
Machine (OSRM) API and enrich data.
"""
from functools import cached_property
from collections.abc import Iterable
from typing import Union, Any, Tuple

from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from FastETL.hooks.osrm_hook import OSRMHook, get_shortest_distance
from FastETL.custom_functions.fast_etl import DbConnection

class OSRMDistanceDbOperator(BaseOperator):
    """Enriches a database with distances calculated using the Open Street
    Routing Machine (OSRM).

    Args:
        db_conn_id (str): The Airflow connection to the database.
        osrm_conn_id (str): The Airflow connection to the OSRM service
            instance.
        table_scheme (str): The database scheme for the table. Used for
            generating the SQL queries.
        table_name (str): The name of the table in the database. Used
            for generating the SQL queries.
        pk_columns (str, ...): One or more column names for the table
            primary keys.
        origin_columns Union[(str, str), str]: Name of the column
            or pair of columns representing the coordinate pair
            (latitude, longitude) for the point of origin.
        destination_columns Union[(str, str), str]: Name of the column
            or pair of columns representing the coordinate pair
            (latitude, longitude) for the point of destination.
        distance_column (str): Name of the column that will be written
            to, with the calculated shortest route distance, in
            kilometers.
        chunksize (int): Size of the chunk read from the database.
    """
    ui_color = '#90d572'

    @apply_defaults
    def __init__(self,
        db_conn_id: str,
        osrm_conn_id: str,
        table_scheme: str,
        table_name: str,
        pk_columns: Tuple[str, ...],
        origin_columns: Union[Tuple[str, str], str],
        destination_columns: Union[Tuple[str, str], str],
        distance_column: str,
        chunksize: int = 100,
        **kwargs):
        super().__init__(**kwargs)
        self.db_conn_id = db_conn_id
        self.osrm_conn_id = osrm_conn_id
        self.table_scheme = table_scheme
        self.table_name = table_name
        self.pk_columns = pk_columns
        self.origin_columns = origin_columns
        self.destination_columns = destination_columns
        self.distance_column = distance_column
        self.chunksize = chunksize
        self.conn_type: Connection = None
        self.osrm_hook: BaseHook = None
        if not (
            (isinstance(origin_columns, Iterable) and len(origin_columns) == 2)
            or isinstance(origin_columns, str)):
            raise ValueError('Argumento origin_columns deve ser o nome de '
                'uma única coluna (tipo geometry) ou uma tupla de 2 '
                'colunas do tipo float (lat e long).')
        if not (
            (isinstance(destination_columns, Iterable) and len(destination_columns) == 2)
            or isinstance(destination_columns, str)):
            raise ValueError('Argumento destination_columns deve ser o nome '
                'de uma única coluna (tipo geometry) ou uma tupla de 2 '
                'colunas do tipo float (lat e long).')
        if not isinstance(distance_column, str):
            raise ValueError('Argumento distance_column deve ser o nome '
                'de uma única coluna (tipo geometry).')

    @cached_property
    def airflow_db_conn(self) -> Connection:
        """The Airflow database connection for the operator."""
        return BaseHook.get_connection(conn_id=self.db_conn_id)

    @cached_property
    def db_conn_type(self) -> str:
        """The connection type for the database."""
        return self.airflow_db_conn.conn_type

    @property
    def select_sql(self) -> str:
        """Create the SELECT SQL query for the points of origin and
        destination.

        Returns:
            str: The SQL query for the SELECT.
        """
        query = 'SELECT '
        where = 'WHERE '

        # add the primary key columns
        query += ', '.join(self.pk_columns) + ', '

        # add the origin coordinate columns
        if isinstance(self.origin_columns, str): # geometry type column
            query += (
                f'{self.origin_columns}.Lat AS origin_latitude, '
                f'{self.origin_columns}.Long AS origin_longitude, '
            )
            where += f'{self.origin_columns} IS NOT NULL '
        else: # latitude and longitude float columns
            query += (
                f'{self.origin_columns[0]} AS origin_latitude, '
                f'{self.origin_columns[1]} AS origin_longitude, '
            )
            where += ' AND '.join(
                (f'{column} IS NOT NULL ' for column in self.origin_columns))

        # add the destination coordinate columns
        if isinstance(self.destination_columns, str): # geometry type column
            query += (
                f'{self.destination_columns}.Lat AS destination_latitude, '
                f'{self.destination_columns}.Long AS destination_longitude '
            )
            where += f'AND {self.destination_columns} IS NOT NULL;'
        else: # latitude and longitude float columns
            query += (
                f'{self.destination_columns[0]} AS destination_latitude, '
                f'{self.destination_columns[1]} AS destination_longitude '
            )
            where += 'AND ' + ' AND '.join((
                f'{column} IS NOT NULL '
                for column in self.destination_columns))

        query += f'FROM {self.table_scheme}.{self.table_name} '
        query += where + ';'

        return query

    def update_sql(self, pk_values: Tuple[Any, ...], distance: float) -> str:
        """Creates the UPDATE SQL query to record the calculated
        distance.

        Args:
            pk_values (Any, ...): The values of the primary keys to use
                for building the UPDATE query.
            distance (float): The value of the distance to be recorded
                in the column.

        Returns:
            str: The SQL query for the UPDATE.
        """
        if len(pk_values) != len(self.pk_columns):
            raise ValueError(f'Got {len(pk_values)} primary key values, '
                f'but there {len(self.pk_columns)} primary keys.')

        query = f'''UPDATE {self.table_scheme}.{self.table_name}
        SET {self.distance_column} = {distance}
        '''

        query += 'WHERE ' + ' AND '.join(
            f"{column} = '{pk_values[index]}'"
            for index, column in enumerate(self.pk_columns)
        )

        query += ';'

        return query

    def _update_row(self, row: list) -> str:
        """Get the value for the distance for a given row.

        Args:
            row (list): The database row returned by the SELECT query.

        Returns:
            str: The UPDATE query for this particular row.
        """
        origin_latitude, origin_longitude = row[-4: -2]
        destination_latitude, destination_longitude = row[-2:]
        primary_keys = row[:len(self.pk_columns)]
        return self.update_sql(
            primary_keys,
            get_shortest_distance(
                self.osrm_hook.get_route(
                    origin=(origin_latitude, origin_longitude),
                    destination=(destination_latitude, destination_longitude)
                )
            ) or 'NULL' # use NULL if the function returns None
        )

    def execute(self, context: dict):
        """Execute the operator."""
        self.osrm_hook = OSRMHook(self.osrm_conn_id)
        with DbConnection(
                conn_id=self.db_conn_id,
                provider=self.db_conn_type.upper()) as read_db_conn:
            with DbConnection(
                    conn_id=self.db_conn_id,
                    provider=self.db_conn_type.upper()) as write_db_conn:
                with read_db_conn.cursor() as select_cursor:
                    select_cursor.execute(self.select_sql)
                    while rows := select_cursor.fetchmany(self.chunksize):
                        for row in rows:
                            with write_db_conn.cursor() as update_cursor:
                                update_cursor.execute(self._update_row(row))
                                update_cursor.commit()
