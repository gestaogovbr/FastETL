# Thanks to Jedi Wash! *.*
"""
Módulo cópia de dados entre Postgres e MsSql e outras coisas
"""

import time
from datetime import datetime, date
import re
import warnings
from typing import Union, Tuple
import logging
import pandas as pd
from pandas.io.sql import DatabaseError
from psycopg2 import OperationalError
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes as sa_types
import sqlalchemy.dialects as sa_dialects

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mssql_hook import MsSqlHook

from FastETL.custom_functions.utils.db_connection import (
    DbConnection,
    get_conn_type,
    get_mssql_odbc_engine,
    get_hook_and_engine_by_provider,
)
from FastETL.custom_functions.utils.load_info import LoadInfo
from FastETL.custom_functions.utils.table_comments import TableComments
from FastETL.custom_functions.utils.get_table_cols_name import (
    get_table_cols_name,
)


def build_select_sql(source_table: str, column_list: str) -> str:
    """
    Monta a string do select da origem
    """

    columns = ", ".join(col for col in column_list)

    return f"SELECT {columns} FROM {source_table}"


def build_dest_sqls(
    destination_table: str, column_list: str, wildcard_symbol: str
) -> Union[str, str, str]:
    """
    Monta a string do insert do destino
    Monta a string de truncate do destino
    """

    columns = ", ".join(f'"{col}"' for col in column_list)

    values = ", ".join([wildcard_symbol for i in range(len(column_list))])
    insert = f"INSERT INTO {destination_table} ({columns}) " f"VALUES ({values})"

    truncate = f"TRUNCATE TABLE {destination_table}"

    return insert, truncate


def insert_df_to_db(
    df: pd.DataFrame,
    conn_id: str,
    schema: str,
    table: str,
    reflect_col_table: bool = True,
):
    """
    Insere os registros do DataFrame df na tabela especificada. Insere
    apenas as colunas que existem na tabela.

    TODO: Implementar aqui o registro no LOG CONTROLE
    """
    if reflect_col_table:
        # Filter existing table columns
        cols = get_table_cols_name(conn_id=conn_id, schema=schema, table=table)
        cols = [col.lower() for col in cols]
        df.columns = df.columns.str.lower()
        df = df[cols]

    df.to_sql(
        name=table,
        schema=schema,
        con=get_mssql_odbc_engine(conn_id),
        if_exists="append",
        index=False,
    )


def validate_db_string(
    source_table: str, destination_table: str, select_sql: str
) -> None:
    """
    Valida se string do banco está no formato schema.table e se tabelas de
    origem e destino possuem o mesmo nome. Se possui select_sql não valida a
    source_table
    """

    assert (
        destination_table.count(".") == 1
    ), "Estrutura tabela destino deve ser str: schema.table"

    if not select_sql:
        assert (
            source_table.count(".") == 1
        ), "Estrutura tabela origem deve ser str: schema.table"

        if source_table.split(".")[1] != destination_table.split(".")[1]:
            warnings.warn("Tabelas de origem e destino com nomes diferentes")


def create_table_if_not_exist(
    source_table: str,
    source_conn_id: str,
    destination_table: str,
    destination_conn_id: str,
    copy_table_comments: bool,
) -> None:
    """Create table on destination if not exists.

    Args:
        source_table (str): Source table name on format schema.table.
        source_conn_id (str): Airflow connection id.
        destination_table (str): Destination table name on format
            schema.table.
        destination_conn_id (str): Airflow connection id.
        copy_table_comments (bool): Flag to copy table and columns
            comments/descriptions.

    Returns:
        None.
    """

    def _convert_column(old_col: Column, db_provider: str) -> Column:
        """Convert column type.

        Args:
            old_col (Column): Column to convert type.
            db_provider (str): Connection type. If `mssql` or `postgres`.

        Returns:
            Column: Column with converted type.
        """

        type_mapping = {
            "NUMERIC": sa_types.Numeric(38, 13),
            "BIT": sa_types.Boolean(),
        }

        if db_provider == "mssql":
            type_mapping["DATETIME"] = sa_dialects.mssql.DATETIME2()

        return Column(
            old_col["name"],
            type_mapping.get(
                str(old_col["type"]._type_affinity()), old_col["type"]._type_affinity()
            ),
        )

    destination_provider = get_conn_type(destination_conn_id)

    ERROR_TABLE_DOES_NOT_EXIST = {
        "mssql": "Invalid object name",
        "postgres": "does not exist",
    }
    _, source_eng = get_hook_and_engine_by_provider(source_conn_id)
    destination_hook, destination_eng = get_hook_and_engine_by_provider(
        destination_conn_id
    )
    try:
        destination_hook.get_pandas_df(f"select * from {destination_table} where 1=2")
    except (DatabaseError, OperationalError, NoSuchModuleError) as db_error:
        if not ERROR_TABLE_DOES_NOT_EXIST[destination_provider] in str(db_error):
            raise db_error
        # Table does not exist so we create it
        source_eng.echo = True
        try:
            insp = reflection.Inspector.from_engine(source_eng)
        except AssertionError as e:
            logging.error(
                "Não é possível criar tabela automaticamente "
                "a partir deste banco de dados. Crie a tabela "
                "manualmente para executar a cópia dos dados. "
            )
            raise e

        s_schema, s_table = source_table.split(".")

        generic_columns = insp.get_columns(s_table, s_schema)
        dest_columns = [
            _convert_column(c, destination_provider) for c in generic_columns
        ]

        destination_meta = MetaData(bind=destination_eng)
        d_schema, d_table = destination_table.split(".")
        Table(d_table, destination_meta, *dest_columns, schema=d_schema)

        destination_meta.create_all(destination_eng)

    if copy_table_comments:
        _copy_table_comments(
            source_conn_id=source_conn_id,
            source_table=source_table,
            destination_conn_id=destination_conn_id,
            destination_table=destination_table,
        )


def _copy_table_comments(
    source_conn_id: str,
    source_table: str,
    destination_conn_id: str,
    destination_table: str,
) -> None:
    """Copy table and colunms comments/descriptions between databases.

    Args:
        source_conn_id (str): Airflow connection id.
        source_table (str): Table str at format schema.table.
        destination_conn_id (str): Airflow connection id.
        destination_table (str): Table str at format schema.table.

    Returns:
        None.
    """

    source_table_comments = TableComments(
        conn_id=source_conn_id, schema_table=source_table
    )

    destination_table_comments = TableComments(
        conn_id=destination_conn_id, schema_table=destination_table
    )

    destination_table_comments.put_table_comments(
        table_comments=source_table_comments.table_comments
    )


def save_load_info(
    source_conn_id: str,
    source_schema_table: str,
    load_type: str,
    dest_conn_id: str,
    log_schema_name: str,
    rows_loaded: int,
):
    """Insert on db ingest metadata, as origin and number of lines loaded.

    Args:
        source_conn_id (str): Source db airflow connection id
        source_schema_table (str): Table string in format `schema.table`
        load_type (str): if "full" or "incremental"
        dest_conn_id (str): Destination db airflow connection id
        log_schema_name (str): Schema where the control data will be stored.
            Defaults to same as destination.
        rows_loaded (int): Number of rows loaded on the transaction.
    """

    load_info = LoadInfo(
        source_conn_id, source_schema_table, load_type, dest_conn_id, log_schema_name
    )

    load_info.save(rows_loaded)


def get_schema_table_from_query(query: str) -> str:
    """Returns schema.table from a query statement.

    Args:
        query (str): sql query statement.

    Returns:
        schema_table (str): table string in format `schema.table`
    """

    # search pattern "from schema.table" on query
    sintax_from = re.search(
        r"from\s+\"?\'?\[?[\w|\.|\"|\'|\]|\]]*\"?\'?\]?", query, re.IGNORECASE
    ).group()
    # split "from " from "schema.table" and get schema.table [-1]
    db_schema_table = sintax_from.split()[-1]
    # clean [, ], ", '
    db_schema_table = re.sub(r"\[|\]|\"|\'", "", db_schema_table)
    # clean "dbo." if exists on dbo.schema.table
    schema_table = ".".join(db_schema_table.split(".")[-2:])

    return schema_table


def copy_db_to_db(
    destination_table: str,
    source_conn_id: str,
    destination_conn_id: str,
    source_table: str = None,
    select_sql: str = None,
    columns_to_ignore: list = [],
    destination_truncate: bool = True,
    chunksize: int = 1000,
    copy_table_comments: bool = False,
    load_type: str = "full",
) -> None:
    """
    Carrega dado do Postgres/MSSQL/MySQL para Postgres/MSSQL com psycopg2 e pyodbc
    copiando todas as colunas e linhas já existentes na tabela de destino.
    Tabela de destino deve ter a mesma estrutura e nome de tabela e colunas
    que a tabela de origem, ou passar um select_sql que tenha as colunas de
    destino.

    Alguns tipos de dados utilizados na tabela destino podem gerar
    problemas na cópia. Esta lista consolida os casos conhecidos:
    * **float**: mude para **numeric(x,y)** ou **decimal(x,y)**
    * **text**: mude para **varchar(max)** ou **nvarchar**
    * para datas: utilize **date** para apenas datas, **datetime** para
    data com hora, e **datetime2** para timestamp

    Exemplo:
        copy_db_to_db('Siorg_VBL.contato_email',
                      'SIORG_ESTATISTICAS.contato_email',
                      'POSTG_CONN_ID',
                      'PG',
                      'MSSQL_CONN_ID',
                      'MSSQL')

    Args:
        destination_table (str): tabela de destino no formato schema.table
        source_conn_id (str): connection origem do Airflow
        destination_conn_id (str): connection destino do Airflow
        source_table (str): tabela de origem no formato schema.table
        select_sql (str): query sql para consulta na origem. Se utilizado o
            source_table será ignorado
        columns_to_ignore (list): list of columns to be ignored in the copy
        destination_truncate (bool): booleano para truncar tabela de destino
            antes do load. Default = True
        chunksize (int): tamanho do bloco de leitura na origem.
            Default = 1000 linhas
        copy_table_comments (bool): flag if includes on the execution the
            copy of table comments/descriptions. Default to False.
        load_type (str): if "full" or "incremental". Default to "full"

    Return:
        None

    Todo:
        * Transformar em Classe ou em Airflow Operator
        * Criar tabela no destino quando não existente
        * Alterar conexão do Postgres para pyodbc
        * Possibilitar inserir data da carga na tabela de destino
        * Criar testes
    """

    # validate db string
    validate_db_string(source_table, destination_table, select_sql)

    # create table if not exists in destination db
    create_table_if_not_exist(
        source_table,
        source_conn_id,
        destination_table,
        destination_conn_id,
        copy_table_comments,
    )

    source_provider = get_conn_type(source_conn_id)
    destination_provider = get_conn_type(destination_conn_id)

    # create connections
    with DbConnection(source_conn_id) as source_conn:
        with DbConnection(destination_conn_id) as destination_conn:
            with source_conn.cursor() as source_cur:
                with destination_conn.cursor() as destination_cur:
                    # Fast etl
                    if destination_provider == "mssql":
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
                        columns_to_ignore=columns_to_ignore,
                    )

                    insert, truncate = build_dest_sqls(
                        destination_table, col_list, wildcard_symbol
                    )
                    if not select_sql:
                        select_sql = build_select_sql(source_table, col_list)

                    # Remove as aspas na query para compatibilidade com o MYSQL
                    if source_provider == "mysql":
                        select_sql = select_sql.replace('"', "")

                    # truncate stg
                    if destination_truncate:
                        destination_cur.execute(truncate)
                        if destination_provider == "mssql":
                            destination_cur.commit()

                    # download data
                    start_time = time.perf_counter()
                    source_cur.execute(select_sql)
                    rows = source_cur.fetchmany(chunksize)
                    rows_inserted = 0

                    logging.info("Inserindo linhas na tabela [%s].", destination_table)
                    while rows:
                        destination_cur.executemany(insert, rows)
                        rows_inserted += len(rows)
                        rows = source_cur.fetchmany(chunksize)
                        logging.info("%d linhas inseridas!!", rows_inserted)

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time

                    if select_sql:
                        source_table = get_schema_table_from_query(select_sql)

                    save_load_info(
                        source_conn_id=source_conn_id,
                        source_schema_table=source_table,
                        load_type=load_type,
                        dest_conn_id=destination_conn_id,
                        log_schema_name=destination_table.split(".")[0],
                        rows_loaded=rows_inserted,
                    )

                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)


def _table_rows_count(db_hook, table: str, where_condition: str = None):
    """Calcula a quantidade de linhas na tabela (table) e utiliza a
    condição (where_condition) caso seja passada como parâmetro.
    """
    sql = f"SELECT COUNT(*) FROM {table}"
    sql += f" WHERE {where_condition};" if where_condition is not None else ";"

    return db_hook.get_first(sql)[0]


def _build_filter_condition(
    dest_hook: MsSqlHook,
    table: str,
    date_column: str,
    key_column: str,
    since_datetime: datetime = None,
) -> Tuple[str, str]:
    """Monta o filtro (where) obtenção o valor max() da tabela,
    distinguindo se a coluna é a "data ou data/hora de atualização"
    (date_column) ou outro número sequencial (key_column), por exemplo
    id, pk, etc. Se o parâmetro "since_datetime" for recebido, será
    considerado em vez do valor max() da tabela.

    Exemplo:
        _build_filter_condition(dest_hook: hook,
                        table=table,
                        date_column=date_column,
                        key_column=key_column)

    Args:
        dest_hook (str): hook de conexão do DB de destino
        table (str): tabela a ser sincronizada
        date_column (str): nome da coluna a ser utilizado para
            identificação dos registros atualizados.
        key_column (str): nome da coluna a ser utilizado como chave na
            etapa de atualização dos registros antigos que sofreram
            atualizações na origem.
        since_datetime (datetime): data/hora a partir do qual o filtro será
            montado, em vez de usar o max() da tabela.

        Returns:
                Tuple[str, str]: Tupla contendo o valor máximo e a condição
                        where da query sql.

    """
    if since_datetime:
        max_value = since_datetime
    else:
        if date_column:
            sql = f"SELECT MAX({date_column}) FROM {table}"
        else:
            sql = f"SELECT MAX({key_column}) FROM {table}"

        max_value = dest_hook.get_first(sql)[0]

    if date_column:
        # Verifica se o formato do campo max_value é date ou datetime
        if type(max_value) == date:
            max_value = max_value.strftime("%Y-%m-%d")
        elif type(max_value) == datetime:
            max_value = max_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        where_condition = f"{date_column} > '{max_value}'"
    else:
        max_value = str(max_value)
        where_condition = f"{key_column} > '{max_value}'"

    return max_value, where_condition


def _build_incremental_sqls(
    dest_table: str, source_table: str, key_column: str, column_list: str
):
    """Constrói as queries SQLs que realizam os Updates dos registros
    atualizados desde a última sincronização e os Inserts das novas
    linhas.
    """
    cols = ", ".join(f"{col} = orig.{col}" for col in column_list)
    updates_sql = f"""
            UPDATE {dest_table} SET {cols}
            FROM {source_table} orig
            WHERE orig.{key_column} = {dest_table}.{key_column}
            """
    cols = ", ".join(column_list)
    inserts_sql = f"""INSERT INTO {dest_table} ({cols})
            SELECT {cols}
            FROM {source_table} AS inc
            WHERE NOT EXISTS
            (SELECT 1 FROM {dest_table} AS atual
                WHERE atual.{key_column} = inc.{key_column})
            """
    return updates_sql, inserts_sql


def sync_db_2_db(
    source_conn_id: str,
    destination_conn_id: str,
    table: str,
    date_column: str,
    key_column: str,
    source_schema: str,
    destination_schema: str,
    increment_schema: str,
    select_sql: str = None,
    since_datetime: datetime = None,
    sync_exclusions: bool = False,
    source_exc_schema: str = None,
    source_exc_table: str = None,
    source_exc_column: str = None,
    chunksize: int = 1000,
    copy_table_comments: bool = False,
) -> None:
    """Realiza a atualização incremental de uma tabela. A sincronização
    é realizada em 3 etapas. 1-Envia as alterações necessárias para uma
    tabela intermediária localizada no esquema `increment_schema`.
    2-Realiza os Updates. 3-Realiza os Inserts. Apenas as colunas que
    existam na tabela no BD destino serão sincronizadas. Funciona com
    Postgres na origem e MsSql no destino. O algoritmo também realiza,
    opcionalmente, sincronização de exclusões.

    Exemplo:
        sync_db_2_db(source_conn_id=SOURCE_CONN_ID,
                     destination_conn_id=DEST_CONN_ID,
                     table=table,
                     date_column=date_column,
                     key_column=key_column,
                     source_schema=SOURCE_SCHEMA,
                     destination_schema=STG_SCHEMA,
                     chunksize=CHUNK_SIZE)

    Args:
        source_conn_id (str): string de conexão airflow do DB origem
        destination_conn_id (str): string de conexão airflow do DB destino
        table (str): tabela a ser sincronizada
        date_column (str): nome da coluna a ser utilizado para
            identificação dos registros atualizados na origem.
        key_column (str): nome da coluna a ser utilizado como chave na
            etapa de atualização dos registros antigos que sofreram
            atualizações na origem.
        source_eschema (str): esquema do BD na origem
        destination_schema (str): esquema do BD no destino
        increment_schema (str): Esquema no banco utilizado para tabelas
            temporárias. Caso esta variável seja None, esta tabela será
            criada no mesmo schema com sufixo '_alteracoes'
        select_sql (str): select customizado para utilizar na carga ao invés
            de replicar as colunas da tabela origem. Não deve ser utilizado com
            JOINS, apenas para uma única tabela.
        since_datetime (datetime): data/hora a partir da qual o incremento
            será realizado, sobrepondo-se à data/hora máxima da tabela destino
        sync_exclusions (bool): opção de sincronizar exclusões.
            Default = False.
        source_exc_schema (str): esquema da tabela na origem onde estão
            registradas exclusões
        source_exc_table (str): tabela na origem onde estão registradas
            exclusões
        source_exc_column (str): coluna na tabela na origem onde estão
            registradas exclusões
        chunksize (int): tamanho do bloco de leitura na origem.
        Default = 1000 linhas
        copy_table_comments (bool): flag if includes on the execution the
            copy of table comments/descriptions. Default to False.

    Return:
        None

    Todo:
        * Automatizar a criação da tabela gêmea e remoção ao final
        * Transformar em Airflow Operator
        * Possibilitar ler de MsSql e escrever em Postgres
        * Possibilitar inserir data da carga na tabela de destino
        * Criar testes
    """

    def _divide_chunks(l, n):
        """Split list into a new list with n lists"""
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i : i + n]

    source_table_name = f"{source_schema}.{table}"
    dest_table_name = f"{destination_schema}.{table}"
    if increment_schema:
        inc_table_name = f"{increment_schema}.{table}"
    else:
        inc_table_name = f"{destination_schema}.{table}_alteracoes"

    source_hook = PostgresHook(postgres_conn_id=source_conn_id, autocommit=True)
    dest_hook, _ = get_hook_and_engine_by_provider(destination_conn_id)

    col_list = get_table_cols_name(destination_conn_id, destination_schema, table)

    dest_rows_count = _table_rows_count(dest_hook, dest_table_name)
    logging.info("Total de linhas atualmente na tabela destino: %d.", dest_rows_count)
    # If de tabela vazia separado para evitar erro na _build_filter_condition()
    if dest_rows_count == 0:
        raise Exception("Tabela destino vazia! Utilize carga full!")

    ref_value, where_condition = _build_filter_condition(
        dest_hook, dest_table_name, date_column, key_column, since_datetime
    )
    new_rows_count = _table_rows_count(source_hook, source_table_name, where_condition)
    logging.info("Total de linhas novas ou modificadas: %d.", new_rows_count)

    # Guarda as alterações e inclusões necessárias
    if not select_sql:
        select_sql = build_select_sql(f"{source_table_name}", col_list)
    select_diff = f"{select_sql} WHERE {where_condition}"
    logging.info("SELECT para espelhamento: %s", select_diff)

    copy_db_to_db(
        destination_table=f"{inc_table_name}",
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        source_table=source_table_name,
        select_sql=select_diff,
        destination_truncate=True,
        chunksize=chunksize,
        load_type="incremental",
    )

    # Reconstrói índices
    destination_conn_type = get_conn_type(destination_conn_id)
    if destination_conn_type == "mssql":
        sql = f"ALTER INDEX ALL ON {inc_table_name} REBUILD"
    elif destination_conn_type == "postgres":
        sql = f"REINDEX TABLE {inc_table_name}"

    dest_hook.run(sql)

    logging.info("Iniciando carga incremental na tabela %s.", dest_table_name)
    updates_sql, inserts_sql = _build_incremental_sqls(
        dest_table=f"{dest_table_name}",
        source_table=f"{inc_table_name}",
        key_column=key_column,
        column_list=col_list,
    )
    # Realiza updates
    dest_hook.run(updates_sql)
    # Realiza inserts de novas linhas
    dest_hook.run(inserts_sql)

    # Se precisar aplicar as exclusões da origem no destino:
    if sync_exclusions:
        source_exc_sql = f"""SELECT {key_column}
                             FROM {source_exc_schema}.{source_exc_table}
                             WHERE {source_exc_column} > '{ref_value}'
                          """
        rows = source_hook.get_records(source_exc_sql)
        ids_to_del = [row[0] for row in rows]

        if ids_to_del:
            ids_to_del_split = _divide_chunks(ids_to_del, 500)
            for chunk in ids_to_del_split:
                ids = ", ".join(str(id) for id in chunk)
                sql = f"""
                    DELETE FROM {dest_table_name}
                    WHERE {key_column} IN ({ids})
                """
                dest_hook.run(sql)

        logging.info(
            "Quantidade de linhas possivelmente excluídas: %d", len(ids_to_del)
        )

    # atualiza comentários da tabela
    if copy_table_comments:
        _copy_table_comments(
            source_conn_id,
            source_table_name,
            destination_conn_id,
            dest_table_name,
        )
