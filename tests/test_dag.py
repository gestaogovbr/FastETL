from datetime import datetime
from airflow import DAG

from FastETL.operators.copy_db_to_db_operator import CopyDbToDbOperator

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
        ('pg', 'public')
    ]
    for source_conf in db_confs:
        for dest_conf in db_confs:
            CopyDbToDbOperator(
                task_id=f'test_from_{source_conf[0]}_to_{dest_conf[0]}',
                source_table=f'{source_conf[1]}.source_table',
                destination_table=f'{dest_conf[1]}.destination_table',
                source_conn_id=f'{source_conf[0]}-source-conn',
                destination_conn_id=f'{dest_conf[0]}-destination-conn',
                )
