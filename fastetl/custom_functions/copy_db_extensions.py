"""
Include database copy extensions.
"""

import time
import logging
import psycopg2
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from fastetl.custom_functions.utils.db_connection import DbConnection, get_conn_type
from fastetl.custom_functions.utils.get_table_cols_name import get_table_cols_name
from fastetl.custom_functions.fast_etl import (
    build_dest_sqls,
    build_select_sql,
)
from fastetl.custom_functions.fast_etl import DestinationConnection


def copy_by_key_interval(
    source_conn_id: str,
    source_table: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 10000,
    estimated_max_id: int = None,
    destination_truncate: bool = True,
):
    """
    Carrega dado do Postgres/MSSQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas já existentes na tabela de destino usando
    intervalos de valores da chave da tabela na consulta à origem.
    Tabela de destino deve ter as mesmas colunas ou subconjunto das colunas
    da tabela de origem, e com data types iguais ou compatíveis.

    Exemplo:
        copy_by_key_interval(
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            key_column='id',
                            key_start=2565,
                            key_interval=1000,
                            destination_truncate=False)

    Args:
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a cópia é iniciada
        key_interval (int): intervalo de id's para ler da origem a cada vez
        estimated_max_id (int): valor estimado do id máximo da tabela
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True

    Return:
        Tupla (status: bool, next_key: int):
            status: True (sucesso) ou False (falha)
            next_key: id da próxima chave a carregar, quando status = False
                      None: quando status = True, ou outras situações

    TODO:
        * try/except nas aberturas das conexões; se erro: return False, key_start
        * comparar performance do prepared statement no source: psycopg2 x pyodbc
    """

    # create connections
    with DbConnection(source_conn_id) as source_conn:
        with DbConnection(destination_conn_id) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:
                    dest_conn_type = get_conn_type(destination_conn_id)
                    # Fast etl
                    if dest_conn_type == "mssql":
                        destination_conn.autocommit = False
                        destination_cur.fast_executemany = True
                        wildcard_symbol = "?"
                    else:
                        wildcard_symbol = "%s"

                    # gera queries
                    col_list = get_table_cols_name(
                        conn_id=destination_conn_id,
                        schema=destination_table.split(".")[0],
                        table=destination_table.split(".")[1],
                    )
                    insert, truncate = build_dest_sqls(
                        destination=DestinationConnection(
                            conn_id=destination_conn_id,
                            schema=destination_table.split(".")[0],
                            table=destination_table.split(".")[1],
                        ),
                        column_list=col_list,
                        wildcard_symbol=wildcard_symbol,
                    )
                    select_sql = build_select_sql(schema=source_table.split(".")[0],
                                                  table=source_table.split(".")[1],
                                                  column_list=col_list)
                    # pyodbc: select_sql = f"{select_sql} WHERE {key_column} BETWEEN ? AND ?"
                    select_sql = f"{select_sql} WHERE {key_column} BETWEEN %s AND %s"

                    # truncate stg
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        destination_cur.commit()

                    # copy by key interval
                    start_time = time.perf_counter()
                    key_begin = key_start
                    key_end = key_begin + key_interval - 1
                    rows_inserted = 0

                    # Tenta LER na origem
                    try:
                        # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchall()
                        source_cur.execute(select_sql, (key_begin, key_end))
                        rows = source_cur.fetchall()  # psycopg2
                    except Exception as e:
                        logging.info(
                            "Erro origem: %s. Key interval: %s-%s",
                            str(e),
                            key_begin,
                            key_end,
                        )

                        return False, key_begin

                    last_sleep = datetime.now()
                    run_step = 60 * 30  # 30 minutos

                    """
                    O loop normal seria com `while rows:`, porém fica sujeito
                    a cargas incompletas quando existem grandes lacunas entre
                    as keys (lacunas maiores do o `key_interval`). Por isso,
                    verifica-se o max_id da tabela.
                    Como exceção, para tabelas em que o max(id) dá timeout na
                    consulta (exemplo: SIADS), usa-se o parâmetro informado
                    `estimated_max_id`.
                    """
                    if estimated_max_id:
                        max_id = estimated_max_id
                    else:
                        # Consulta max id na origem
                        db_hook = PostgresHook(postgres_conn_id=source_conn_id)
                        max_id_sql = f"""select COALESCE(max({key_column}),0)
                                            FROM {source_table}"""
                        max_id = int(db_hook.get_first(max_id_sql)[0])

                    while key_begin <= max_id and max_id > 0:
                        if (datetime.now() - last_sleep).seconds > run_step:
                            logging.info(
                                "Roda por %d segundos e dorme por 20 segundos "
                                "para evitar o erro Negsignal.SIGKILL do Airflow!",
                                run_step,
                            )
                            time.sleep(20)
                            last_sleep = datetime.now()

                        # Tenta ESCREVER no destino
                        if rows:
                            try:
                                if dest_conn_type == "postgres":
                                    psycopg2.extras.execute_batch(destination_cur, insert, rows)
                                else:
                                    destination_cur.executemany(insert, rows)
                                destination_conn.commit()
                            except Exception as e:
                                logging.info(
                                    "Erro destino: %s. Key interval: %s-%s",
                                    str(e),
                                    key_begin,
                                    key_end,
                                )

                                return False, key_begin

                        rows_inserted += len(rows)
                        key_begin = key_end + 1
                        key_end = key_begin + key_interval - 1
                        # Tenta LER na origem
                        try:
                            # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchall()
                            source_cur.execute(select_sql, (key_begin, key_end))
                            rows = source_cur.fetchall()  # psycopg2
                        except Exception as e:
                            logging.info(
                                "Erro origem: %s. Key interval: %s-%s",
                                str(e),
                                key_begin,
                                key_end,
                            )

                            return False, key_begin

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time
                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)

                    return True, None


def copy_by_key_with_retry(
    source_conn_id: str,
    source_table: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 10000,
    estimated_max_id: int = None,
    destination_truncate: bool = True,
    retries: int = 0,
    retry_delay: int = 600,
):
    """
    Copia tabela entre dois bancos de dados chamando a function
    copy_by_key_interval(), mas permitindo retries quando ocorre falha.
    Uso: cópia de tabelas grandes do Quartzo em que se verifica frequentes
    falhas durante os fetchs, por exemplo, no vdb Siasg.
    A vantagem dos retries da function é não precisar fazer count na tabela
    destino a cada nova tentativa de restart. Em tabelas grandes, o count
    demora para retornar.

    Exemplo:
        copy_by_key_with_retry(
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            key_column='id',
                            key_start=8150500,
                            key_interval=10000,
                            destination_truncate=False,
                            retries=10,
                            retry_delay=300)

    Args:
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a cópia é iniciada
        key_interval (int): intervalo de id's para ler da origem a cada vez
        estimated_max_id (int): valor estimado do id máximo da tabela
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
        retries (int): número máximo de tentativas de reprocessamento
        retry_delay (int): quantos segundos aguardar antes da nova tentativa
    """

    retry = 0
    succeeded, next_key = copy_by_key_interval(
        source_conn_id=source_conn_id,
        source_table=source_table,
        destination_conn_id=destination_conn_id,
        destination_table=destination_table,
        key_column=key_column,
        key_start=key_start,
        key_interval=key_interval,
        estimated_max_id=estimated_max_id,
        destination_truncate=destination_truncate,
    )

    while not succeeded and (retry <= retries):
        logging.info("Falha na function copy_by_key_interval !!!")
        retry += 1
        logging.info("Tentando retry %d em %d segundos...", retry, retry_delay)
        time.sleep(retry_delay)
        succeeded, next_key = copy_by_key_interval(
            source_conn_id=source_conn_id,
            source_table=source_table,
            destination_conn_id=destination_conn_id,
            destination_table=destination_table,
            key_column=key_column,
            key_start=next_key,
            key_interval=key_interval,
            estimated_max_id=estimated_max_id,
            destination_truncate=False,
        )

    if succeeded:
        logging.info("Término com sucesso!")
    else:
        logging.info("Término com erro após %d tentativas!", retries)


def copy_by_limit_offset(
    source_conn_id: str,
    source_table: str,
    destination_conn_id: str,
    destination_table: str,
    limit: int = 1000,
    destination_truncate: bool = True,
):
    """
    Carrega dado do Postgres/MSSQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas já existentes na tabela de destino, usando
    blocagem de linhas limit/offset na consulta à origem.
    Tabela de destino deve ter as mesmas colunas ou subconjunto das colunas
    da tabela de origem, e com data types iguais ou compatíveis.

    Exemplo:
        copy_by_limit_offset(
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            limit=1000,
                            destination_truncate=True)

    Args:
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        limit (int): quantidade de linhas para ler da origem a cada vez
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
    """

    # create connections
    with DbConnection(source_conn_id) as source_conn:
        with DbConnection(destination_conn_id) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:
                    # Fast etl
                    destination_conn.autocommit = False
                    destination_cur.fast_executemany = True

                    # gera queries com limit e offset
                    col_list = get_table_cols_name(
                        conn_id=destination_conn_id,
                        schema=destination_table.split(".")[0],
                        table=destination_table.split(".")[1],
                    )
                    insert, truncate = build_dest_sqls(
                        destination=DestinationConnection(
                            conn_id=destination_conn_id,
                            schema=destination_table.split(".")[0],
                            table=destination_table.split(".")[1],
                        ),
                        column_list=col_list,
                        wildcard_symbol="?",
                    )
                    select_sql = build_select_sql(schema=source_table.split(".")[0],
                                                  table=source_table.split(".")[1],
                                                  column_list=col_list)
                    # pyodbc: select_sql = f"{select_sql} limit ?, ?"
                    select_sql = f"{select_sql} limit %s, %s"

                    # truncate stg
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        destination_cur.commit()

                    # copy by limit offset
                    start_time = time.perf_counter()
                    next_offset = 0
                    rows_inserted = 0
                    # pyodbc: rows = source_cur.execute(select_sql, next_offset, limit).fetchall()
                    source_cur.execute(select_sql, (next_offset, limit))
                    rows = source_cur.fetchall()  # psycopg2

                    while rows:
                        destination_cur.executemany(insert, rows)
                        destination_conn.commit()
                        rows_inserted += len(rows)
                        next_offset = next_offset + limit
                        # pyodbc: rows = source_cur.execute(select_sql, next_offset, limit).fetchall()
                        source_cur.execute(select_sql, (next_offset, limit))
                        rows = source_cur.fetchall()  # psycopg2

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time
                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)
