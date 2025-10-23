"""Data types commonly used across other modules."""

from typing import Dict, Optional, TypedDict


class DBSource(TypedDict):
    """A database source. Used in functions, hooks and operators.

    A dictionary containing the connection details on the source database.

        Depending on full or incremental copy, specific keys can be passed
        on the source dictionary.

        (full copy)
        source full copy dict expects these keys:
        * conn_id -> required
        * schema and table -> required if `query` not provided.
        * query -> required if `schema` and `table` not provided. Can be
            the direct query string or a path to a .sql file.
        * query_params -> optional parameters to be substituted in the
            query template.

        (incremental copy)
        source incremental copy dict expects these keys:
        * conn_id -> required
        * schema -> required
        * query -> optional. Can be the direct query string or a path to
            a .sql file.
        * query_params -> optional parameters to be substituted in the
            query template.
        * source_exc_schema -> optional
            Table `schema` name at the source where exclusions are recorded.
        * source_exc_table -> optional
            Table `table` name at the source where exclusions are recorded.
        * source_exc_column -> optional
            Table `column` name at the source where exclusions are recorded.
    """
    conn_id: str
    schema: Optional[str]
    schema_name: Optional[str] # deprecated, for backwards compatibility
    table: Optional[str]
    query: Optional[str]
    query_params: Optional[Dict[str, str]]
    source_exc_schema: Optional[str]
    source_exc_table: Optional[str]
    source_exc_column: Optional[str]
