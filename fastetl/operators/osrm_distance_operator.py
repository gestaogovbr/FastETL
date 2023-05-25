"""Airflow operators to calculate distances with the Open Street Routing
Machine (OSRM) API and enrich data.
"""
from functools import cached_property
from collections.abc import Iterable
from typing import Union, Tuple

import pandas as pd

from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from airflow.models.baseoperator import BaseOperator

from fastetl.hooks.osrm_hook import OSRMHook, get_shortest_distance
from fastetl.custom_functions.utils.db_connection import DbConnection

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
        overwrite_existing (bool): If true, will always overwrite the
            contents of the distance column. If false, will only
            calculate distances and fill in for lines that have NULL
            values in that column.
    """
    ui_color = '#90d572'

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
        overwrite_existing: bool = True,
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
        self.overwrite_existing = overwrite_existing
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
            where += (' AND '.join((
                f'{column} IS NOT NULL ' for column in self.origin_columns
                )) + ' ')

        # add the destination coordinate columns
        if isinstance(self.destination_columns, str): # geometry type column
            query += (
                f'{self.destination_columns}.Lat AS destination_latitude, '
                f'{self.destination_columns}.Long AS destination_longitude '
            )
            where += f'AND {self.destination_columns} IS NOT NULL '
        else: # latitude and longitude float columns
            query += (
                f'{self.destination_columns[0]} AS destination_latitude, '
                f'{self.destination_columns[1]} AS destination_longitude '
            )
            where += ('AND ' + ' AND '.join((
                f'{column} IS NOT NULL ' for column in self.destination_columns
                )) + ' ')

        query += f'FROM {self.table_scheme}.{self.table_name} '

        if self.db_conn_type == 'mssql':
            query += 'WITH (NOLOCK) '

        if not self.overwrite_existing:
            where += f'AND {self.distance_column} IS NULL '

        query += where + ';'

        return query

    def update_sql(self, data: pd.DataFrame) -> str:
        """Creates the UPDATE SQL query to record the calculated
        distance.

        Args:
            data (pd.DataFrame): The data for building the UPDATE query,
                composed of columns for the primary keys and the
                calculated distances.

        Returns:
            str: The SQL query for the UPDATE.
        """
        if len(data.columns) - 1 != len(self.pk_columns):
            raise ValueError(f'Got {len(data.columns) - 1} primary key '
                'values, but there are '
                f'{len(self.pk_columns)} primary keys.')

        query = (
            f'UPDATE {self.table_scheme}.{self.table_name} ' +
            f'SET {self.distance_column} = r.calculated_distance ' +
            f'FROM {self.table_scheme}.{self.table_name} s ' +
            'JOIN (' +
            ' UNION ALL '.join((
                'SELECT ' +
                ', '.join((
                    f"'{getattr(row, primary_key)}' AS {primary_key}"
                    for primary_key in self.pk_columns
                )) +
                f', {row.calculated_distance} AS calculated_distance'
                    for row in data.itertuples()
            )) +
            ') r ' +
            'ON ' +
            ' AND '.join((
                f's.{primary_key} = r.{primary_key} '
                for primary_key in self.pk_columns
            )) + ';'
        )

        return query

    def _update_rows(self, rows: pd.DataFrame) -> str:
        """Get the value for the distance for a given row.

        Args:
            rows (pd.DataFrame): A dataframe of database rows returned
            by the SELECT query.

        Returns:
            str: The UPDATE query for this particular row.
        """
        rows['calculated_distance'] = rows.apply(
            lambda row:
            get_shortest_distance(
                self.osrm_hook.get_route(
                    origin=(row.origin_latitude, row.origin_longitude),
                    destination=(row.destination_latitude,
                        row.destination_longitude),
                    steps=False
                )
            ) or 'NULL', # use NULL if the function returns None
            axis=1
        )
        return self.update_sql(rows[list(self.pk_columns) + ['calculated_distance']])

    def execute(self, context: dict):
        """Execute the operator."""
        self.osrm_hook = OSRMHook(self.osrm_conn_id)
        with DbConnection(
                conn_id=self.db_conn_id) as read_db_conn:
            with DbConnection(
                    conn_id=self.db_conn_id) as write_db_conn:
                for rows in pd.read_sql(
                        self.select_sql,
                        con=read_db_conn,
                        chunksize=self.chunksize):
                    with write_db_conn.cursor() as update_cursor:
                        update_cursor.execute(self._update_rows(rows))
                        update_cursor.commit()
