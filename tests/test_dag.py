from datetime import datetime
from airflow import DAG

from fastetl.operators.db_to_db_operator import DbToDbOperator

default_args = {
    'owner': 'nitai',
    'start_date': datetime(2020, 8, 20),
    'depends_on_past': False,
}
with DAG(
        "test_dag",
        default_args=default_args,
        catchup=False,
    ) as dag:
    db_confs = [
        ('mssql', 'dbo'),
        ('postgres', 'public')
    ]
    for source_conf in db_confs:
        for dest_conf in db_confs:
            DbToDbOperator(
                task_id=f'test_from_{source_conf[0]}_to_{dest_conf[0]}',
                source={
                    "conn_id": f'{source_conf[0]}-source-conn',
                    "schema": source_conf[1],
                    "table": 'source_table',
                },
                destination={
                    "conn_id": f'{dest_conf[0]}-destination-conn',
                    "schema": dest_conf[1],
                    "table": 'destination_table',
                },
            )
