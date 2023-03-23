"""
Module for handling tables in Open Document Format (ODF) text documents
and generating textual data dictionaries.
"""
from typing import List

from frictionless import Package

from odf import text as odf_text, table as odf_table, teletype
from odf.opendocument import OpenDocument

def get_table_by_name(doc: OpenDocument, name: str) -> odf_table.Table:
    """Returns a table object with the given name from an OpenDocument
    Text document.

    Args:
        doc (OpenDocument): the OpenDocument object to search in.
        name (str): name attribute of the table

    Returns:
        odf_table.Table: a table object with the given name
    """
    return [
        doc_table
        for doc_table in doc.getElementsByType(odf_table.Table)
        if doc_table.getAttribute("name") == name
    ][0]

def add_table_rows(doc_table: odf_table.Table, rows: List[List[str]]) -> odf_table.Table:
    """Adds a row to an OpenDocument text format table.

    The formatting on the table cells and paragraphs is preserved. The
    table should already contain a header and a first row. The first row
    is used just to get the style for the new rows and is deleted after
    the process.

    The list passed as `row` should have a size equal to the number of
    existing columns on the table.

    Args:
        doc_table (odf_table.Table): the table object
        row (List[List[str]]): a list of lists of strings, one for each
            table cell

    Returns:
        odf_table.Table: the updated table object
    """
    # Get the first table row under the table header
    first_row = doc_table.getElementsByType(odf_table.TableRow)[1]
    row_style = first_row.getAttribute("stylename")
    first_cell = first_row.getElementsByType(odf_table.TableCell)[0]
    cell_style = first_cell.getAttribute("stylename")

    columns_in_table = len(doc_table.getElementsByType(odf_table.TableColumn))
    for row in rows:
        # Check the number of columns
        if columns_in_table != len(row):
            raise ValueError(
                f"Incompatible data: table has {columns_in_table:d} columns "
                f"and row has {len(row):d} values."
            )
        table_row = odf_table.TableRow()
        for text in row:
            table_cell = odf_table.TableCell(
                stylename=cell_style, valuetype="string", value=text
            )
            table_row.addElement(table_cell)
            table_cell.addElement(odf_text.P(stylename=row_style, text=text))
            doc_table.addElement(table_row)

    # remove the first row (was just a sample)
    first_row.parentNode.removeChild(first_row)
    return doc_table

def create_data_dictionary(doc_template: str, data_package: str, output: str):
    """Creates a data dictionary from an ODT document template.

    Args:
        doc_template (str): path to the odf file used as a template
        data_package (str): path to the data package descriptor
        output (str): path to the resulting odf file
    """
    doc = load(doc_template)
    package = Package(data_package)
    for resource in package.resources:
        doc_table = get_table_by_name(doc, resource.name)
        rows = []
        for field in resource.schema.fields:
            rows.append([field.name, field.type, field.description])
        doc_table = add_table_rows(doc_table, rows)
    doc.save(output)
