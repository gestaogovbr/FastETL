"""Tests for the OSRMDbOperator.

Currently it's just an example dag using the operator.

TODO:
- set up OSRM container for the tests
- write proper tests with pytest
"""

from datetime import datetime, timedelta

from airflow.decorators import dag

from fastetl.operators.osrm_distance_operator import OSRMDistanceDbOperator

# need to create and configure Airflow connections
MSSQL_STAGE_CONN_ID = "mssql_srv"
OSRM_CONN_ID = 'osrm_api'

# DAG
args = {
    'owner': 'fastetl',
    'start_date': datetime(2022, 6, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(hours=1)
}
@dag(
    default_args=args,
    schedule=None,
    catchup=False,
    description=__doc__,
    tags=["osrm"]
)
def osrm_test():
    calculate_distance = OSRMDistanceDbOperator(
        task_id='calculate_distance',
        db_conn_id=MSSQL_STAGE_CONN_ID,
        osrm_conn_id=OSRM_CONN_ID,
        table_scheme='SCHEME',
        table_name='TABLE',
        pk_columns=('key1', 'key2'),
        origin_columns='origin',
        destination_columns='destination',
        distance_column='km_calculated_route',
    )

dag = osrm_test()
