"""
Get database table columns names.
"""

from typing import List

from FastETL.custom_functions.utils.db_connection import DbConnection


def get_table_cols_name(
    conn_id: str, schema: str, table: str, columns_to_ignore: List = []
) -> List[str]:
    """
    Obtem a lista de colunas de uma tabela.
    """

    with DbConnection(conn_id) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {schema}.{table} WHERE 1=2")
            column_names = [tup[0] for tup in cur.description]

    column_names = [n for n in column_names if n not in columns_to_ignore]

    return column_names
