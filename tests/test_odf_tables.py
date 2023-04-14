"""Tests associated with the custom_functions.utils.odf_tables module.
"""
from typing import List
import yaml
import os
import pytest

from frictionless import Package
import odf.teletype, odf.opendocument

from fastetl.custom_functions.utils.odf_tables import (
    DocumentWithTables,
    create_data_dictionary,
)

TABLE_TITLE = "Countries"
TABLE_DESCRIPTION = "List of country names and international codes."
COLUMN_NAMES = ["field name", "type", "description"]
TABLE_DATA = [
    ["contry name", "string", "name of the country"],
    ["contry code", "string", "ISO 3166-1 alpha-2 two letter code"],
    ["contry number", "integer", "ISO 3166-1 numeric three-digit code"],
]
PACKAGE_DESCRIPTOR = f"""
profile: tabular-data-package
resources:
  -
    name: countries
    title: {TABLE_TITLE}
    description: {TABLE_DESCRIPTION}
    profile: tabular-data-resource
    path: some-file.csv
    format: csv
    mediatype: text/csv
    encoding: utf-8
    schema:
      fields:
""" + "\n".join(
    f"""
        -
          name: {field[0]}
          title: {field[0]}
          type: {field[1]}
          description: {field[2]}
""".strip("\n")
    for field in TABLE_DATA
)
TEMP_DOCUMENT_NAME = "Test data dictionary.odt"


@pytest.mark.parametrize(
    "columns, table_data, table_title, table_description",
    [(COLUMN_NAMES, TABLE_DATA, TABLE_TITLE, TABLE_DESCRIPTION)],
)
def test_create_new_table_document(
    columns: List[str],
    table_data: List[List[str]],
    table_title: str,
    table_description: str,
):
    document = DocumentWithTables()
    document.append_table(
        "countries",
        column_names=columns,
        title=table_title,
        description=table_description,
    )
    document.tables["countries"].add_rows(table_data)

    # verify contents of document
    assert odf.teletype.extractText(document.odf_document.text) == "".join(
        (
            TABLE_TITLE,
            TABLE_DESCRIPTION,
            "".join(COLUMN_NAMES),
            "".join("".join(cell) for cell in TABLE_DATA),
        )
    )


@pytest.mark.parametrize(
    "data_package_descriptor",
    [(PACKAGE_DESCRIPTOR)],
)
def test_create_new_data_dictionary(data_package_descriptor: str):
    descriptor = yaml.safe_load(data_package_descriptor)
    create_data_dictionary(Package(descriptor), TEMP_DOCUMENT_NAME)

    # verify contents of document file
    document = odf.opendocument.load(TEMP_DOCUMENT_NAME)
    assert odf.teletype.extractText(document.text) == "".join(
        (
            TABLE_TITLE,
            TABLE_DESCRIPTION,
            "".join(COLUMN_NAMES),
            "".join("".join(cell) for cell in TABLE_DATA),
        )
    )
    os.remove(TEMP_DOCUMENT_NAME)
