# Thanks to Jedi Wash! *.*
"""
Módulo cópia de dados entre Postgres e MsSql e outras coisas
"""

import time
from datetime import datetime, date
import re
from typing import Union, Tuple, Dict
import logging
import pandas as pd
from pandas.io.sql import DatabaseError
from psycopg2 import OperationalError
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes as sa_types
import sqlalchemy.dialects as sa_dialects

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from fastetl.custom_functions.utils.db_connection import (
    DbConnection,
    get_conn_type,
    get_mssql_odbc_engine,
    get_hook_and_engine_by_provider,
)
from fastetl.custom_functions.utils.load_info import LoadInfo
from fastetl.custom_functions.utils.table_comments import TableComments
from fastetl.custom_functions.utils.get_table_cols_name import (
    get_table_cols_name,
)


class SourceConnection:
    """
    Represents a source connection to a database, encapsulating the connection details
    (e.g., connection ID, schema, table, query) required to read data from a database.

    Args:
        conn_id (str): The unique identifier of the connection to use.
        schema (str, optional): The name of the schema to use. Default is None.
        table (str, optional): The name of the table to use. Default is None.
        query (str, optional): The SQL query to use. Default is None.

    Raises:
        ValueError: If `conn_id` is empty or if neither `query` nor (`schema` and `table`)
            is provided.

    Attributes:
        conn_id (str): The unique identifier of the connection.
        schema (str): The name of the schema.
        table (str): The name of the table.
        query (str): The SQL query.
    """

    def __init__(
        self, conn_id: str, schema: str = None, table: str = None, query: str = None
    ):

        if not conn_id:
            raise ValueError("conn_id argument cannot be empty")
        if not query and not (schema or table):
            raise ValueError("must provide either schema and table or query")

        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.query = query


class DestinationConnection:
    """
    Represents a destination connection to a database, encapsulating the connection details
    (e.g., connection ID, schema, table) required to write data to a database.

    Args:
        conn_id (str): The unique identifier of the connection to use.
        schema (str): The name of the schema to use.
        table (str): The name of the table to use.

    Attributes:
        conn_id (str): The unique identifier of the connection.
        schema (str): The name of the schema.
        table (str): The name of the table.
    """

    def __init__(self, conn_id: str, schema: str, table: str):

        self.conn_id = conn_id
        self.schema = schema
        self.table = table


def build_select_sql(schema: str, table: str, column_list: str) -> str:
    """
    Monta a string do select da origem
    """

    columns = ", ".join(f'"{col}"' for col in column_list)

    return f"SELECT {columns} FROM {schema}.{table}"


def build_dest_sqls(
    destination: DestinationConnection, column_list: str, wildcard_symbol: str
) -> Union[str, str, str]:
    """
    Monta a string do insert do destino
    Monta a string de truncate do destino
    """

    columns = ", ".join(f'"{col}"' for col in column_list)

    values = ", ".join([wildcard_symbol for i in range(len(column_list))])
    insert = (
        f"INSERT INTO {destination.schema}.{destination.table} ({columns}) "
        f"VALUES ({values})"
    )

    truncate = f"TRUNCATE TABLE {destination.schema}.{destination.table}"

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


def create_table_if_not_exists(
    source: SourceConnection,
    destination: DestinationConnection,
    copy_table_comments: bool,
) -> None:
    """
    Creates a destination table if it does not already exist and copies
    data from a source table to the destination.

    Args:
        source (SourceConnection): A `SourceConnection` object containing
            the connection details for the source database.
        destination (DestinationConnection): A `DestinationConnection`
            object containing the connection details for the destination
            database.
        copy_table_comments (bool): A flag indicating whether to copy table
            and columns comments/descriptions.

    Returns:
        None.

    Raises:
        DatabaseError: If there is an error with the database connection or
            query.
        OperationalError: If there is an error with the database operation.
        NoSuchModuleError: If a required module is missing.
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

    destination_provider = get_conn_type(destination.conn_id)

    ERROR_TABLE_DOES_NOT_EXIST = {
        "mssql": "Invalid object name",
        "postgres": "does not exist",
    }
    _, source_eng = get_hook_and_engine_by_provider(source.conn_id)
    destination_hook, destination_eng = get_hook_and_engine_by_provider(
        destination.conn_id
    )
    try:
        destination_hook.get_pandas_df(
            f"select * from {destination.schema}.{destination.table} where 1=2"
        )
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

        generic_columns = insp.get_columns(source.table, source.schema)
        dest_columns = [
            _convert_column(c, destination_provider) for c in generic_columns
        ]

        destination_meta = MetaData(bind=destination_eng)
        Table(
            destination.table,
            destination_meta,
            *dest_columns,
            schema=destination.schema,
        )

        destination_meta.create_all(destination_eng)

    if copy_table_comments:
        _copy_table_comments(
            source=source,
            destination=destination,
        )


def _copy_table_comments(
    source: SourceConnection, destination: DestinationConnection
) -> None:
    """
    Copy table and column comments/descriptions between databases.

    Args:
        source (SourceConnection): Connection object containing the
            source database information.
        destination (DestinationConnection): Connection object
            containing the destination database information.

    Returns:
        None.
    """

    source_table_comments = TableComments(
        conn_id=source.conn_id, schema=source.schema, table=source.table
    )

    destination_table_comments = TableComments(
        conn_id=destination.conn_id, schema=destination.schema, table=destination.table
    )

    destination_table_comments.put_table_comments(
        table_comments=source_table_comments.table_comments
    )


def save_load_info(
    source: SourceConnection,
    destination: DestinationConnection,
    load_type: str,
    rows_loaded: int,
):
    """
    Inserts metadata information into a database about a data ingestion
    process, including the origin of the data, the type of ingestion
    (full or incremental), the destination database, the schema where
    the control data will be stored, and the number of rows loaded.

    Args:
        source (SourceConnection): Object with connection info to the
            source database (conn_id, schema, table).
        destination (DestinationConnection): Object with connection info
            to the destination database (conn_id, schema).
        load_type (str): Type of data ingestion, "full" or "incremental".
        rows_loaded (int): Number of rows loaded in the transaction.

    Returns:
        None.
    """

    load_info = LoadInfo(
        source_conn_id=source.conn_id,
        source_schema=source.schema,
        source_table=source.table,
        load_type=load_type,
        dest_conn_id=destination.conn_id,
        log_schema_name=destination.schema,
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
    schema, table = db_schema_table.split(".")[-2:]

    return schema, table


def copy_db_to_db(
    source: Dict[str, str],
    destination: Dict[str, str],
    columns_to_ignore: list = None,
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
        copy_db_to_db(
            {"conn_id": "conn_id", "schema": "schema", "table: "table"},
            {"conn_id": "conn_id", "schema": "schema", "table: "table"}
        )

    Args:
        source (Dict[str, str]): A dictionary containing connection
            information for the source database.
            conn_id (str): Airflow connection id.
            schema (str): Source information `schema` name.
            table (str): Source information `table` name.

            source dict expects these keys:
                * conn_id -> required
                * schema and table -> required if `query` not provided.
                * query -> required if `schema` and `table` not provided.

        destination (Dict[str, str]): A dictionary containing connection
            information for the destination database.
            conn_id (str): Airflow connection id.
            schema (str): Destination information `schema` name.
            table (str): Destination information `table` name.

            destination dict expects these keys:
                * conn_id -> required
                * schema -> required
                * table -> required

        columns_to_ignore (list, optional): A list of column names to
            ignore during the copy operation. Defaults to None.
        destination_truncate (bool, optional): If True, the destination
            table will be truncated before copying data. Defaults to True.
        chunksize (int, optional): The number of rows to copy at once.
            Defaults to 1000.
        copy_table_comments (bool, optional): If True, comments on the
            source table will be copied to the destination table.
            Defaults to False.
        load_type (str, optional): The type of load to perform. Can be
            "full" or "incremental". Defaults to "full".

    Return:
        None

    Todo:
        * Transformar em Classe ou em Airflow Operator
        * Criar tabela no destino quando não existente
        * Alterar conexão do Postgres para pyodbc
        * Possibilitar inserir data da carga na tabela de destino
        * Criar testes
    """

    # validate connections
    source = SourceConnection(**source)
    destination = DestinationConnection(**destination)

    # create table if not exists in destination db
    create_table_if_not_exists(
        source,
        destination,
        copy_table_comments,
    )

    source_provider = get_conn_type(source.conn_id)
    destination_provider = get_conn_type(destination.conn_id)

    # create connections
    with DbConnection(source.conn_id) as source_conn:
        with DbConnection(destination.conn_id) as destination_conn:
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
                        conn_id=destination.conn_id,
                        schema=destination.schema,
                        table=destination.table,
                        columns_to_ignore=columns_to_ignore,
                    )

                    insert, truncate = build_dest_sqls(
                        destination, col_list, wildcard_symbol
                    )
                    if source.query:
                        select_sql = source.query
                    else:
                        select_sql = build_select_sql(
                            schema=source.schema,
                            table=source.table,
                            column_list=col_list,
                        )

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

                    logging.info(
                        "Inserindo linhas na tabela [%s].[%s]",
                        destination.schema,
                        destination.table,
                    )
                    while rows:
                        destination_cur.executemany(insert, rows)
                        rows_inserted += len(rows)
                        rows = source_cur.fetchmany(chunksize)
                        logging.info("%d linhas inseridas!!", rows_inserted)

                    destination_conn.commit()

                    delta_time = time.perf_counter() - start_time

                    if source.query:
                        source.schema, source.table = get_schema_table_from_query(
                            source.query
                        )

                    save_load_info(
                        source=source,
                        destination=destination,
                        load_type=load_type,
                        rows_loaded=rows_inserted,
                    )

                    logging.info("Tempo load: %f segundos", delta_time)
                    logging.info("Linhas inseridas: %d", rows_inserted)
                    logging.info("linhas/segundo: %f", rows_inserted / delta_time)


def _table_rows_count(db_hook, table: str, where_condition: str = None):
    """
    Calcula a quantidade de linhas na tabela (table) e utiliza a
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
    """
    Monta o filtro (where) obtenção o valor max() da tabela,
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
        if isinstance(max_value, date):
            max_value = max_value.strftime("%Y-%m-%d")
        elif isinstance(max_value, datetime):
            max_value = max_value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        where_condition = f"{date_column} > '{max_value}'"
    else:
        max_value = str(max_value)
        where_condition = f"{key_column} > '{max_value}'"

    return max_value, where_condition


def _build_incremental_sqls(
    dest_table: str, source_table: str, key_column: str, column_list: str
):
    """
    Constrói as queries SQLs que realizam os Updates dos registros
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
    """
    Realiza a atualização incremental de uma tabela. A sincronização
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
        select_sql = build_select_sql(
            schema=source_schema, table=table, column_list=col_list
        )
    select_diff = f"{select_sql} WHERE {where_condition}"
    logging.info("SELECT para espelhamento: %s", select_diff)

    copy_db_to_db(
        source={
            "conn_id": source_conn_id,
            "query": select_diff,
            "schema": source_table_name.split(".")[0],
            "table": source_table_name.split(".")[1],
        },
        destination={
            "conn_id": destination_conn_id,
            "schema": inc_table_name.split(".")[0],
            "table": inc_table_name.split(".")[1],
        },
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
            source=SourceConnection(
                conn_id=source_conn_id,
                schema=source_table_name.split(".")[0],
                table=source_table_name.split(".")[1],
            ),
            destination=DestinationConnection(
                conn_id=destination_conn_id,
                schema=dest_table_name.split(".")[0],
                table=dest_table_name.split(".")[1],
            ),
        )
