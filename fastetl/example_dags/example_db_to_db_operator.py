from datetime import datetime
from airflow import DAG
from fastetl.operators.db_to_db_operator import DbToDbOperator

default_args = {
    "start_date": datetime(2023, 4, 1),
}

dag = DAG(
    "copy_db_to_db_example",
    default_args=default_args,
    schedule=None,
)

t0 = DbToDbOperator(
    task_id="copy_data",
    source={
        "conn_id": "airflow_source_conn_id",
        "schema": "source_schema",
        "table": "table_name",
    },
    destination={
        "conn_id": "airflow_dest_conn_id",
        "schema": "dest_schema",
        "table": "table_name",
    },
    destination_truncate=True,
    copy_table_comments=True,
    chunksize=10000,
    dag=dag,
)