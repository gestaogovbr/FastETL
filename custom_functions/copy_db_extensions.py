"""
_summary_
"""

import time
import logging
from datetime import datetime
import ctds
import ctds.pool

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.hooks.mssql_hook import MsSqlHook

from FastETL.custom_functions.fast_etl import (
    DbConnection,
    validate_db_string,
    get_cols_name,
    build_dest_sqls,
    build_select_sql,
)

# XXX utilizado em carga_inicial_grandes_tabelas


def copy_by_key_interval(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 10000,
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
                            source_provider='PG',
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_provider='MSSQL',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            key_column='id',
                            key_start=2565,
                            key_interval=1000,
                            destination_truncate=False)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a cópia é iniciada
        key_interval (int): intervalo de id's para ler da origem a cada vez
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

    # validate db string
    validate_db_string(source_table, destination_table, None)

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:

                    # Fast etl
                    if destination_provider == "MSSQL":
                        destination_conn.autocommit = False
                        destination_cur.fast_executemany = True
                        wildcard_symbol = "?"
                    else:
                        wildcard_symbol = "%s"

                    # gera queries
                    col_list = get_cols_name(
                        destination_cur, destination_provider, destination_table
                    )
                    insert, truncate = build_dest_sqls(
                        destination_table, col_list, wildcard_symbol
                    )
                    select_sql = build_select_sql(source_table, col_list)
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

                    # Consulta max id na origem
                    db_hook = PostgresHook(postgres_conn_id=source_conn_id)
                    max_id_sql = f"""select COALESCE(max({key_column}),0)
                                        FROM {source_table}"""
                    max_id = int(db_hook.get_first(max_id_sql)[0])

                    # while rows:
                    while key_begin <= max_id and max_id > 0:
                        if (datetime.now() - last_sleep).seconds > run_step:
                            logging.info(
                                "Roda por %d segundos e dorme por 20 segundos para evitar o erro Negsignal.SIGKILL do Airflow!",
                                run_step,
                            )
                            time.sleep(20)
                            last_sleep = datetime.now()

                        # Tenta ESCREVER no destino
                        if rows:
                            try:
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
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 10000,
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
                            source_provider='PG',
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_provider='MSSQL',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            key_column='id',
                            key_start=8150500,
                            key_interval=10000,
                            destination_truncate=False,
                            retries=10,
                            retry_delay=300)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a cópia é iniciada
        key_interval (int): intervalo de id's para ler da origem a cada vez
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
        retries (int): número máximo de tentativas de reprocessamento
        retry_delay (int): quantos segundos aguardar antes da nova tentativa
    """

    retry = 0
    succeeded, next_key = copy_by_key_interval(
        source_provider=source_provider,
        source_conn_id=source_conn_id,
        source_table=source_table,
        destination_provider=destination_provider,
        destination_conn_id=destination_conn_id,
        destination_table=destination_table,
        key_column=key_column,
        key_start=key_start,
        key_interval=key_interval,
        destination_truncate=destination_truncate,
    )

    while not succeeded and (retry <= retries):
        logging.info("Falha na function copy_by_key_interval !!!")
        retry += 1
        logging.info("Tentando retry %d em %d segundos...", retry, retry_delay)
        time.sleep(retry_delay)
        succeeded, next_key = copy_by_key_interval(
            source_provider=source_provider,
            source_conn_id=source_conn_id,
            source_table=source_table,
            destination_provider=destination_provider,
            destination_conn_id=destination_conn_id,
            destination_table=destination_table,
            key_column=key_column,
            key_start=next_key,
            key_interval=key_interval,
            destination_truncate=False,
        )

    if succeeded:
        logging.info("Término com sucesso!")
    else:
        logging.info("Término com erro após %d tentativas!", retries)


######################################################
# XXX não utilizados em nenhum lugar


def copy_by_limit_offset(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
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
                            source_provider='PG',
                            source_conn_id='quartzo_scdp',
                            source_table='novoScdp_VBL.trecho',
                            destination_provider='MSSQL',
                            destination_conn_id='mssql_srv_30_stg_scdp',
                            destination_table='dbo.trecho',
                            limit=1000,
                            destination_truncate=True)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        limit (int): quantidade de linhas para ler da origem a cada vez
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
    """

    # validate db string
    validate_db_string(source_table, destination_table, None)

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:

                    # Fast etl
                    destination_conn.autocommit = False
                    destination_cur.fast_executemany = True

                    # gera queries com limit e offset
                    col_list = get_cols_name(
                        destination_cur, destination_provider, destination_table
                    )
                    insert, truncate = build_dest_sqls(destination_table, col_list, "?")
                    select_sql = build_select_sql(source_table, col_list)
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


def search_key_gaps(
    source_provider: str,
    source_conn_id: str,
    source_table: str,
    destination_provider: str,
    destination_conn_id: str,
    destination_table: str,
    key_column: str,
    key_start: int = 0,
    key_interval: int = 100,
):
    """
    Verifica se existem lacunas de linhas entre intervalos de chaves,
    comparando tabela origem x tabela destino. Para cada intervalo, deve
    existir a mesma quantidade distinta de id's.

    Exemplo:
        search_key_gaps(
                        source_provider='PG',
                        source_conn_id='quartzo_comprasnet',
                        source_table='Comprasnet_VBL.tbl_pregao',
                        destination_provider='MSSQL',
                        destination_conn_id='mssql_srv_32_stg_comprasnet',
                        destination_table='dbo.tbl_pregao',
                        key_column='prgCod',
                        key_start=0,
                        key_interval=100000)

    Args:
        source_provider (str): provider do banco origem (MSSQL ou PG)
        source_conn_id (str): connection origem do Airflow
        source_table (str): tabela de origem no formato schema.table
        destination_provider (str): provider do banco destino (MSSQL ou PG)
        destination_conn_id (str): connection destino do Airflow
        destination_table (str): tabela de destino no formato schema.table
        key_column (str): nome da coluna chave da tabela origem
        key_start (int): id da chave a partir do qual a comparação é feita
        key_interval (int): intervalo de id's para ler da origem a cada vez
    """
    # validate db string
    validate_db_string(source_table, destination_table, None)

    # create connections
    with DbConnection(source_conn_id, source_provider) as source_conn:
        with DbConnection(
            destination_conn_id, destination_provider
        ) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:

                    # gera queries
                    select_sql = f"""
                        SELECT COUNT(DISTINCT {key_column}) AS count_source
                        FROM {source_table}"""
                    # pyodbc: select_sql = f"{select_sql} WHERE {key_column} BETWEEN ? AND ?"
                    select_sql = f"{select_sql} WHERE {key_column} BETWEEN %s AND %s"
                    compare_sql = f"""
                        SELECT COUNT(DISTINCT {key_column}) AS count_dest
                        FROM {destination_table}"""
                    compare_sql = f"{compare_sql} WHERE {key_column} BETWEEN ? AND ?"
                    # psycopg2: compare_sql = f"{compare_sql} WHERE {key_column} BETWEEN %s AND %s"
                    lastkey_sql = f"""
                        SELECT MAX({key_column}) AS last_key
                        FROM {destination_table}"""

                    # compare by key interval
                    start_time = time.perf_counter()

                    rowlast = destination_cur.execute(lastkey_sql).fetchone()
                    # psycopg2: destination_cur.execute(lastkey_sql)
                    # psycopg2: rowlast = destination_cur.fetchone()
                    last_key = rowlast.last_key

                    gaps = totdif = 0
                    key_begin = key_start
                    key_end = key_begin + key_interval - 1
                    # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchone()
                    source_cur.execute(select_sql, (key_begin, key_end))
                    rows = source_cur.fetchone()

                    while rows:
                        rowsdest = destination_cur.execute(
                            compare_sql, key_begin, key_end
                        ).fetchone()
                        # psycopg2: destination_cur.execute(compare_sql, (key_begin, key_end))
                        # psycopg2: rowsdest = destination_cur.fetchone()
                        logging.info(
                            "Key interval: %d to %d. %s",
                            key_begin,
                            key_end,
                            time.strftime("%H:%M:%S", time.localtime()),
                        )
                        # pyodbc: count_source = rows.count_source
                        count_source = rows[0]  # psycopg2
                        if count_source != rowsdest.count_dest:
                            dif = count_source - rowsdest.count_dest
                            logging.info(
                                "Gap!!! Source keys: %d. Dest keys: %d. Difference: %d.",
                                count_source,
                                rowsdest.count_dest,
                                dif,
                            )
                            gaps += 1
                            totdif += dif
                        key_begin = key_end + 1
                        key_end = key_begin + key_interval - 1
                        if key_begin > last_key:
                            break
                        # pyodbc: rows = source_cur.execute(select_sql, key_begin, key_end).fetchone()
                        source_cur.execute(select_sql, (key_begin, key_end))
                        rows = source_cur.fetchone()

                    delta_time = time.perf_counter() - start_time
                    logging.info("Tempo do compare: %f segundos", delta_time)
                    logging.info(
                        "Resumo: %d gaps, %d rows faltam no destino!", gaps, totdif
                    )


def _build_increm_filter(
    col_list: list, dest_hook: MsSqlHook, table: str, key_column: str
) -> str:
    """Constrói a condição where (where_condition) a ser utilizada para
    calcular e identificar as linhas da tabela no BD origem (Quartzo/Serpro)
    que devem ser sincronizadas com aquela tabela no BD destino. Se a
    tabela não possuir a coluna 'dataalteracao' será utilizada a coluna
    (key_column).
    """
    col_list = [col.lower() for col in col_list]
    if "dataalteracao" in col_list:
        key = "dataalteracao"
    else:
        key = key_column
    max_value = _table_column_max_string(dest_hook, table, key)
    where_condition = f"{key} > '{max_value}'"

    return where_condition


# TODO: Propor ao Wash de passarmos a definir explicitamente a coluna
# 'dataalteracao' na variável airflow de configurações sempre que for o
# caso, e assim pararmos de checar se a coluna 'dataalteracao' está na
# lista de colunas. Consultá-lo sobre drawbacks.


def _table_column_max_string(db_hook: MsSqlHook, table: str, column: str):
    """Calcula o valor máximo da coluna (column) na tabela (table). Se
    a coluna for 'dataalteracao' a string retornada é formatada.
    """
    sql = f"SELECT MAX({column}) FROM {table};"
    max_value = db_hook.get_first(sql)[0]
    # TODO: Descobrir se é data pelo tipo do BD
    if column == "dataalteracao":
        return max_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    else:
        return str(max_value)


def write_ctds(table, rows, conn_id):
    """
    Escreve em banco MsSql Server utilizando biblioteca ctds com driver
    FreeTds.
    """

    conn_values = BaseHook.get_connection(conn_id)

    dbconfig = {
        "server": conn_values.host,
        "port": int(conn_values.port),
        "database": conn_values.schema,
        "autocommit": True,
        "enable_bcp": True,
        "ntlmv2": True,
        "user": conn_values.login,
        "password": conn_values.password,
        "timeout": 300,
    }

    pool = ctds.pool.ConnectionPool(ctds, dbconfig)

    with pool.connection() as conn:
        itime = time.perf_counter()
        linhas = conn.bulk_insert(
            table=table,
            rows=rows,
            # batch_size=300000,
        )
        ftime = time.perf_counter()
        logging.info("%d linhas inseridas em %f segundos.", linhas, ftime - itime)
