"""
Airflow operator that performs data copying between DBs with complete
or incremental strategy. The DBs can be Postgres, SQL Server or MySQL.
It uses psycopg2 and pyodbc libraries. The copied data can
come from a table or an SQL Query.

Args:
    is_incremental (bool, optional): Whether to perform an incremental
        copy based on a datetime or key column. Defaults to False.

    (when full copy)
    columns_to_ignore (List[str], optional): A list of column names to
        ignore during the copy operation. Defaults to None.
    destination_truncate (bool, optional): Whether to truncate the
        destination table before copying data to it. Defaults to True.

    (when incremental copy)
    table (str, optional): The name of the table to copy in incremental
        mode. Defaults to None.
    date_column (str, optional): The name of the datetime column to use for
        incremental copying. Defaults to None.
    key_column (str, optional): The name of the key column to use for
        incremental copying. Defaults to None.
    since_datetime (datetime.datetime, optional): The datetime from
        which to start the incremental copy. Defaults to None.
    sync_exclusions (bool, optional): Whether to exclude some columns during
        incremental copying. Defaults to False.

    (both full and incremental)
    source (DBSource): A typed dictionary containing connection information
        for the source database.
        See documentation on fastetl.data_types.DBSource for details.

    destination (Dict[str, str]): A dictionary containing the connection
        details of the destination database.

        Depending on full or incremental copy, specific keys can be passed
        on the destination dictionary.

        (full copy)
        destination full copy dict expects these keys:
        * conn_id -> required
        * schema -> required
        * table -> required

        (incremental copy)
        source incremental copy dict expects these keys:
        * conn_id -> required
        * schema -> required
        * increment_schema -> optional
            Schema in the database used for temporary tables. If this
            variable is None, this table will be created in the same
            schema with the suffix '_alteracoes'.

    chunksize (int, optional): The number of rows to fetch from the source
        database at once. Defaults to 1000.
    copy_table_comments (bool, optional): Whether to copy table comments
        from the source database to the destination database.
        Defaults to False.
    debug_mode (bool, optional): Whether to enable debug mode. Defaults to False.

Raises:
    TypeError: If `source` or `destination` is not a dictionary.
"""

from datetime import datetime
import logging
import os
import random
from types import SimpleNamespace
from typing import Dict

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

from fastetl.hooks.db_to_db_hook import DbToDbHook
from fastetl.data_types import DBSource


class DbToDbOperator(BaseOperator):

    def __init__(
        self,
        source: DBSource,
        destination: Dict[str, str],
        columns_to_ignore: list = None,
        destination_truncate: bool = True,
        destination_create: bool = True,
        chunksize: int = 1000,
        copy_table_comments: bool = False,
        is_incremental: bool = False,
        table: str = None,
        date_column: str = None,
        until_column: str = None,
        key_column: str = None,
        since_datetime: datetime = None,
        sync_exclusions: bool = False,
        debug_mode: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.columns_to_ignore = columns_to_ignore
        self.destination_truncate = destination_truncate
        self.destination_create = destination_create
        self.chunksize = chunksize
        self.copy_table_comments = copy_table_comments
        self.is_incremental = is_incremental
        self.table = table
        self.date_column = date_column
        self.until_column = until_column
        self.key_column = key_column
        self.since_datetime = since_datetime
        self.sync_exclusions = sync_exclusions
        self.debug_mode = debug_mode

        # rename if schema_name is present (backwards compatibility)
        if source.get("schema_name", None):
            logging.warning(
                'Deprecated parameter schema_name="%s" was provided in source. '
                "Support will be removed in a future version. "
                'Use schema="%s" instead.',
                source["schema_name"],
                source["schema_name"],
            )
            source["schema"] = source.get("schema_name")
        if destination.get("schema_name", None):
            logging.warning(
                'Deprecated parameter schema_name="%s" was provided in destination. '
                "Support will be removed in a future version. "
                'Use schema="%s" instead.',
                destination["schema_name"],
                destination["schema_name"],
            )
            destination["schema"] = destination.pop("schema_name")
        self.source = source
        self.destination = destination

        # any value that needs to be the same for inlets and outlets
        key = str(random.randint(10000000, 99999999))
        if source.get("om_service", None):
            self.inlets = [OMEntity(entity=Table, fqn=self._get_fqn(source), key=key)]
        if destination.get("om_service", None):
            self.outlets = [
                OMEntity(entity=Table, fqn=self._get_fqn(destination), key=key)
            ]

    def _get_fqn(self, data):
        data["database"] = BaseHook.get_connection(data["conn_id"]).schema
        fqn = (
            f'{data["om_service"]}.{data["database"]}.{data["schema"]}.{data["table"]}'
        )
        return fqn

    def _is_query_a_file(self):
        """Check if the provided query is a file path to a .sql file."""
        if "\n" not in self.source["query"] and self.source["query"].endswith(".sql"):
            # str looks like a file path
            if not os.path.exists(self.source["query"]):
                raise ValueError(f"Template file not found: {self.source['query']}")
            return True
        return False

    def _read_and_expand_query_template(self, str_or_file_path: str, context):
        """Read the file contents and expand the Jinja2 template, if it
        is a template.

        Args:
            str_or_file_path (str): template string or path to the SQL file
                template.
            context (_type_): Airflow's context for expanding a template.
        """
        if self.source.get("query", False) and self._is_query_a_file():
            with open(str_or_file_path, "r", encoding="utf-8") as f:
                template = f.read()
        else:
            template = str_or_file_path

        # return the expanded Jinja2 template
        params = self.source.get("query_params", dict())
        context_with_params = {**context, **{"params": SimpleNamespace(**params)}}
        return self.render_template(template, context=context_with_params)

    def execute(self, context):
        """Execute the operator.

        Args:
            context (dict): Airflow's context.
        """
        # if query is provided, check and process template logic
        if self.source.get("query", False):
            self.source["query"] = self._read_and_expand_query_template(
                self.source["query"], context
            )

        hook = DbToDbHook(
            source=self.source,
            destination=self.destination,
        )

        if self.is_incremental:
            hook.incremental_copy(
                table=self.table,
                date_column=self.date_column,
                until_column=self.until_column,
                key_column=self.key_column,
                since_datetime=self.since_datetime,
                until_datetime=self.until_datetime,
                sync_exclusions=self.sync_exclusions,
                chunksize=self.chunksize,
                copy_table_comments=self.copy_table_comments,
                debug_mode=self.debug_mode,
            )
        else:
            hook.full_copy(
                columns_to_ignore=self.columns_to_ignore,
                destination_truncate=self.destination_truncate,
                destination_create=self.destination_create,
                chunksize=self.chunksize,
                copy_table_comments=self.copy_table_comments,
                debug_mode=self.debug_mode,
            )
