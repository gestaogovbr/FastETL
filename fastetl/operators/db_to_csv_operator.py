"""
Module contains the class

DbToCSVOperator
    Operador that executes a SQL query, generates a CSV file with the
    result and stores the file in the file system.
"""

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from fastetl.custom_functions.utils.get_table_cols_name import get_table_cols_name
from fastetl.custom_functions.utils.db_connection import get_hook_and_engine_by_provider


class DbToCSVOperator(BaseOperator):
    """
    Operador that executes a SQL query, generates a CSV file with the
    result and stores the file in the file system.

    Args:
        conn_id (str): Airflow conn_id of the database where the
            `select_sql` query will be run.
        target_file_dir (str): path to the directory where the target
            file will be created.
        file_name (str): name of the file to be created.
        compression (str | dict): compression parameter to be passed
            along to Panda's to_csv method. Defaults to "infer".
        select_sql (Optional[str]): query string, or path to a file
            containing the query string, that will select and return the
            data to be recorded in the CSV file. If omitted (or None),
            will build a select query containing the specified columns.
            Defaults to None.
        table_name (Optional[str]): name of the table to dynamically build
            the query. Must be used alternatively to the `select_sql`
            argument. Defaults to None.
        table_scheme (Optional[str]): name of the schema to be used to
            dynamically build the query. Must be used alternatively to
            the `select_sql` argument in tandem with the `table_name`
            argument. Defaults to None.
        characters_to_remove (Optional[str]): if specified, the characters
            specified in this string will be removed from any string type
            columns in the dataframe, before exporting to CSV. Defaults
            to None.
        columns_to_remove (Optional[list[str]]): must be used together with
            the `table_name` and `table_scheme` arguments to except the
            columns which won't be extracted to the CSV. Defaults to None.
        int_columns (Optional[list[str]]): list with the names of the
            columns that are of type integer to generate the CSV file
            correctly. Defaults to None.
    """

    ui_color = "#95aad5"
    ui_fgcolor = "#000000"
    template_fields = ("select_sql", "target_file_dir", "file_name")

    def __init__(
        self,
        *args,
        conn_id: str,
        target_file_dir: str,
        file_name: str,
        compression: str | dict = "infer",
        select_sql: Optional[str] = None,
        table_name: Optional[str] = None,
        table_scheme: Optional[str] = None,
        characters_to_remove: Optional[str] = None,
        columns_to_remove: Optional[list[str]] = None,
        int_columns: Optional[list[str]] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.conn_id: str = conn_id
        self.target_file_dir: str = target_file_dir
        self.file_name: str = file_name
        self.compression: str | dict = compression
        self.select_sql: Optional[str] = select_sql
        self.table_name: Optional[str] = table_name
        self.table_scheme: Optional[str] = table_scheme
        self.characters_to_remove: Optional[str] = characters_to_remove
        self.columns_to_remove: Optional[list[str]] = columns_to_remove
        self.int_columns: Optional[list[str]] = int_columns

    def _select_all_sql(self) -> str:
        """Generate a SELECT statement to fetch all columns from
        a table.

        Returns:
            str: a SELECT statement for all columns
        """
        if any(argument is None for argument in (self.table_scheme, self.table_name)):
            raise ValueError(
                "table_scheme and table_name are required "
                "when select_sql is not provided."
            )
        cols = get_table_cols_name(self.conn_id, self.table_scheme, self.table_name)
        if self.columns_to_remove:
            cols = [c for c in cols if c not in self.columns_to_remove]

        return f"""
            SELECT
            {','.join(cols)}
            FROM {self.table_scheme}.{self.table_name};
            """

    def _starts_with_sql_keyword(self, value: str) -> bool:
        """Check if a string starts with a SQL keyword."""
        sql_keywords = ("SELECT", "WITH", "INSERT", "UPDATE", "DELETE")
        return any(value.strip().upper().startswith(kw) for kw in sql_keywords)

    def _resolve_select_sql(self, select_sql: Optional[str | Path]) -> str:
        """Resolve select_sql to a query string, handling files and inline
        queries.

        Args:
            select_sql (Optional[str | Path]): The SQL query or file path to
                resolve. If a string, it can be either a file path or an inline
                SQL query. If a file path, the file must exist.

        Raises:
            FileNotFoundError: if a file path is specified but the file does not
                exist.
            TypeError: if select_sql is neither a string nor a Path object.
            ValueError: if select_sql is None or empty and table_scheme or
                table_name are not provided.

        Returns:
            str: the resolved SQL query string.
        """

        if not select_sql:
            return self._select_all_sql()

        # Handle Path objects
        if isinstance(select_sql, Path):
            if select_sql.is_file():
                return select_sql.read_text(encoding="utf-8")
            raise FileNotFoundError(f"File not found: {select_sql}")

        # Handle strings
        if isinstance(select_sql, str):
            # Quick check: SQL keywords indicate it's a query, not a path
            if self._starts_with_sql_keyword(select_sql):
                return select_sql

            # Attempt to load as a file path
            try:
                path = Path(select_sql)
                if path.is_file():
                    return path.read_text(encoding="utf-8")
            except OSError:
                # Catches "File name too long" and other path-related OS errors
                # Treat as a query string instead
                pass

            # Default: treat as a query string
            return select_sql

        raise TypeError(f"select_sql must be a string or Path, got {type(select_sql)}")

    def execute(self, context: Context):
        """Executes the SQL query and saves the CSV file. In the process,
        converts data types to integers and removes specified characters
        if the options have been specified.

        Args:
            context (Context): The Airflow context object.

        Returns:
            str: The name of the written file.
        """
        _ = context  # left unused
        db_hook, _ = get_hook_and_engine_by_provider(self.conn_id)

        df_select = self._resolve_select_sql(self.select_sql)

        self.log.info(f"Executing SQL check: {df_select}")
        df = db_hook.get_pandas_df(df_select)

        # Convert columns data types to int
        if self.int_columns:
            for col in self.int_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Remove specified characters
        if self.characters_to_remove:
            str_cols = df.select_dtypes(["object"]).columns
            for col in str_cols:
                df[col] = df[col].str.replace(self.characters_to_remove, "")

        # Remove specified columns
        if self.columns_to_remove:
            df.drop(self.columns_to_remove, axis=1, errors="ignore", inplace=True)

        # Create folder if not exists
        if not os.path.exists(self.target_file_dir):
            os.mkdir(self.target_file_dir)

        file_path = os.path.join(self.target_file_dir, self.file_name)
        df.to_csv(file_path, index=False, compression=self.compression)
        return self.file_name
