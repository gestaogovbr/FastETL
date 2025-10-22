from datetime import datetime
import os

from airflow import DAG

from fastetl.operators.db_to_db_operator import DbToDbOperator

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(CURRENT_PATH, "sql")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 8, 20),
    "depends_on_past": False,
}
with DAG(
    "test_dag",
    default_args=default_args,
    catchup=False,
) as dag:
    db_confs = [("mssql", "dbo"), ("postgres", "public")]
    for source_conf in db_confs:
        for dest_conf in db_confs:
            DbToDbOperator(
                task_id=f"test_from_{source_conf[0]}_to_{dest_conf[0]}",
                source={
                    "conn_id": f"{source_conf[0]}-source-conn",
                    "schema": source_conf[1],
                    "table": "source_table",
                },
                destination={
                    "conn_id": f"{dest_conf[0]}-destination-conn",
                    "schema": dest_conf[1],
                    "table": "destination_table",
                },
            )
        for dest_conf in db_confs:
            # Using a query template file
            DbToDbOperator(
                task_id=f"test_from_{source_conf[0]}_query_template_file_to_{dest_conf[0]}",
                source={
                    "conn_id": f"{source_conf[0]}-source-conn",
                    "query": os.path.join(SQL_PATH, "test_query.sql"),
                    "query_params": {
                        "schema": source_conf[1],
                    },
                },
                destination={
                    "conn_id": f"{dest_conf[0]}-destination-conn",
                    "schema": dest_conf[1],
                    "table": "destination_table",
                },
            )
            # Using a query template string
            DbToDbOperator(
                task_id=f"test_from_{source_conf[0]}_query_template_to_{dest_conf[0]}",
                source={
                    "conn_id": f"{source_conf[0]}-source-conn",
                    "query": "SELECT * FROM {{ params.schema }}.source_table",
                    "query_params": {
                        "schema": source_conf[1],
                    },
                },
                destination={
                    "conn_id": f"{dest_conf[0]}-destination-conn",
                    "schema": dest_conf[1],
                    "table": "destination_table",
                },
            )
            # Using query strings
            DbToDbOperator(
                task_id=f"test_from_{source_conf[0]}_query_to_{dest_conf[0]}",
                source={
                    "conn_id": f"{source_conf[0]}-source-conn",
                    "query": f"SELECT * FROM {source_conf[1]}.source_table",
                },
                destination={
                    "conn_id": f"{dest_conf[0]}-destination-conn",
                    "schema": dest_conf[1],
                    "table": "destination_table",
                },
            )
