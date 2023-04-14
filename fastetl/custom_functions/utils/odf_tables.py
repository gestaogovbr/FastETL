"""
Module for handling tables in Open Document Format (ODF) text documents
and generating textual data dictionaries.
"""
from typing import List
import logging

from frictionless import Package, Resource

import odf
import odf.table, odf.text
from odf.opendocument import OpenDocumentText

DATA_DICT_COLUMN_NAMES = {
    "en": ["field name", "type", "description"],
    "pt": ["nome do campo", "tipo", "descrição"],
}


class DocDataTable:
    def __init__(
        self,
        document: "DocumentWithTables",
        name: str,
        title: str = None,
        description: str = None,
        column_names: List[str] = None,
    ):
        """Creates a new DocDataTable object. It is meant as a
        simplified interface for writing tables inside OpenDocument text
        files.

        If the column_names argument is specified, will create a new
        table and append it to the document.

        Args:
            document (DocumentWithTables): the DocumentWithTables object
                the table belongs to.
            name (str): the name of the table (same as in the OpenDocument
                text document property).
            title (str, optional): the table's title (text of the heading
                immediately preceding the table). Optional.
                Defaults to None.
            description (str, optional): the table's description (a block of text
                immediately preceding the table). Optional.
                Defaults to None.
            column_names (List[str], optional): List of column names.
                Defaults to None. If specified, will create a new table
                and append it to the document.
        """
        self.document = document
        self.name = name
        self.title = title
        self.description = description
        self.column_names = column_names
        self.table = None
        if column_names:
            self.create_table(column_names)

    def create_title_and_description(self, level: int = 3):
        """Appends to the document a heading with the table's title and
        a block of text containing the table's description, if available.

        Args:
            level (int, optional): level of the heading for the title.
                Defaults to 3.
        """
        if self.title:
            heading = odf.text.H(outlinelevel=3, text=self.title)
            self.document.odf_document.text.addElement(heading)
        if self.description:
            paragraph = odf.text.P(text=self.description)
            self.document.odf_document.text.addElement(paragraph)

    def create_table(self, column_names: List[str], include_title: bool = True):
        """Appends to the document a new table containing a header row
        with the provided column names.

        Args:
            column_names (List[str]): list of names of the table columns.
                Will be inserted in the header row of the table.
            include_title (bool): whether or not to include a heading and
                text preceding the table, containing a table title and
                description, if available. Defaults to True.
        """
        if include_title:
            self.create_title_and_description()
        doc_table = odf.table.Table()
        self.table = doc_table
        self.document.odf_document.text.addElement(doc_table)
        column = odf.table.TableColumn(numbercolumnsrepeated=len(column_names))
        doc_table.addElement(column)
        header_rows = odf.table.TableHeaderRows()
        doc_table.addElement(header_rows)
        header_row = odf.table.TableRow()
        header_rows.addElement(header_row)
        for column_name in column_names:
            cell = odf.table.TableCell(valuetype="string", value=column_name)
            cell.addElement(odf.text.P(text=column_name))
            header_row.addElement(cell)

    @classmethod
    def load_from_doc(cls, document: "DocumentWithTables", name: str) -> "DocDataTable":
        """Creates a DocDataTable instance by loading it from the
        OpenDocument text document referenced in document.

        Args:
            document (DocumentWithTables): the DocumentWithTables where the table is.
            name (str): name of the table to load.

        Returns:
            DocDataTable: a new instance of DocDataTable pointing to
                the existing table.
        """
        data_table = cls(document=document, name=name)
        for doc_table in data_table.document.odf_document.getElementsByType(
            odf.table.Table
        ):
            if doc_table.getAttribute("name") == name:
                data_table.table = doc_table
        return data_table

    @staticmethod
    def get_number_of_columns(table: odf.table.Table) -> int:
        """Returns the number of columns in a table.

        Args:
            table (odf.table.Table): the Table object to inspect.

        Returns:
            int: number of columns.
        """
        columns = 0
        for table_column in table.getElementsByType(odf.table.TableColumn):
            repeated = table_column.getAttribute("numbercolumnsrepeated")
            if repeated:
                columns += int(repeated)
            else:
                columns += 1
        return columns

    def add_rows(
        self,
        rows: List[List[str]],
        use_template_row: bool = False,
        delete_template_row: bool = False,
    ):
        """Adds rows to a DocDataTable.

        The formatting on the table cells and paragraphs is preserved.
        The table should already contain a header and a first row. The
        first row is used just to get the style for the new rows and is
        deleted after the process.

        Each inner list passed in `rows` should have a size equal to the
        number of existing columns on the table.

        Args:
            row (List[List[str]]): a list of strings, one for each table
                cell
            use_template_row (bool): if True, will copy the style of
                the first row of the table to all of the created rows.
                Defaults to False.
            delete_template_row (bool): if True, will delete the
                previously existing first row in the template, which is
                used only for copying the styles. Defaults to False.

        """
        if use_template_row:
            # Get the first table row under the table header
            first_row = self.table.getElementsByType(odf.table.TableRow)[1]
            row_style = first_row.getAttribute("stylename")
            first_cell = first_row.getElementsByType(odf.table.TableCell)[0]
            cell_style = first_cell.getAttribute("stylename")
        else:
            row_style = self.document.get_style("Standard")
            cell_style = self.document.get_style("Standard")

        columns_in_table = self.get_number_of_columns(self.table)
        # Check the number of columns
        for row in rows:
            if columns_in_table != len(row):
                raise ValueError(
                    f"Incompatible data: table has {columns_in_table:d} columns "
                    f"and row has {len(row):d} values."
                )
            table_row = odf.table.TableRow()
            for text in row:
                table_cell = odf.table.TableCell(
                    stylename=cell_style, valuetype="string", value=text
                )
                table_row.addElement(table_cell)
                table_cell.addElement(odf.text.P(stylename=row_style, text=text))
                self.table.addElement(table_row)

        if delete_template_row:
            # remove the first preexisting template row
            first_row.parentNode.removeChild(first_row)


class DocumentWithTables:
    """An OpenDocument text format document that contains tables.
    Includes methods for adding, manipulating and facilitating access to
    the tables.
    """

    def __init__(self, create_new: bool = True):
        """Creates a new DocumentWithTables from scratch.

        Args:
            create_new (bool, optional): If true, will instantiate a new
                OpenDocument object and add default styles.
                Defaults to True.
        """
        self.doc_template = None
        self.odf_document = None
        self.tables = {}
        if create_new:
            self.create()

    @classmethod
    def load_from_template(
        cls, doc_template: str, load_all_tables: bool = False
    ) -> "DocumentWithTables":
        """Creates a DocumentWithTables from an existing template file.

        Args:
            doc_template (str): path to the document template file, in
                OpenDocumentText text format.
            load_all_tables (bool, optional): if True, will pre-load all
                existing tables in the template file. Defaults to False.
        """
        document = cls(create_new=False)
        document.doc_template = doc_template
        document.odf_document = odf.opendocument.load(doc_template)
        if load_all_tables:
            tables_in_doc = document.odf_document.getElementsByType(odf.table.Table)
            for doc_table in tables_in_doc:
                table_name = doc_table.getAttribute("name")
                if table_name:
                    document.tables[table_name] = DocDataTable.load_from_doc(
                        document, table_name
                    )
        return document

    def create(self):
        """Creates an new odfpy OpenDocument object with the default
        styles.
        """
        self.odf_document = OpenDocumentText()
        self.create_default_styles()

    def get_style(self, name: str) -> odf.element.Element:
        """Try to get a style from a document by name

        Args:
            name (str): style's name.

        Returns:
            odf.element.Element: the Style object, if found, or None.
        """
        try:
            return self.odf_document.getStyleByName(name)
        except AssertionError:
            return None

    def create_default_styles(self):
        """Create some default styles for the document."""
        document = self.odf_document
        if self.get_style("Standard") is None:
            style = odf.style.Style(
                name="Standard", family="paragraph", attributes={"class": "text"}
            )
            p = odf.style.ParagraphProperties(
                margintop="0.423cm", marginbottom="0.212cm", keepwithnext="always"
            )
            style.addElement(p)
            p = odf.style.TextProperties(
                fontname="Sawasdee",
                fontsize="12pt",
                fontnameasian="DejaVu LGC Sans",
                fontsizeasian="12pt",
                fontnamecomplex="DejaVu LGC Sans",
                fontsizecomplex="12pt",
            )
            style.addElement(p)
            document.styles.addElement(style)

        if self.get_style("Table text") is None:
            style = odf.style.Style(
                name="Table text",
                displayname="Text body",
                family="paragraph",
                parentstylename="Standard",
                attributes={"class": "text"},
            )
            p = odf.style.ParagraphProperties(margintop="0cm", marginbottom="0.212cm")
            style.addElement(p)
            p = odf.style.TextProperties(
                fontname="Liberation Sans Narrow",
                fontsize="12pt",
                fontnameasian="DejaVu LGC Sans",
                fontsizeasian="12pt",
                fontnamecomplex="DejaVu LGC Sans",
                fontsizecomplex="12pt",
            )
            style.addElement(p)
            document.styles.addElement(style)

        if self.get_style("Heading") is None:
            style = odf.style.Style(
                name="Heading",
                family="paragraph",
                parentstylename="Standard",
                nextstylename="Standard",
                attributes={"class": "text"},
            )
            p = odf.style.ParagraphProperties(
                margintop="0.423cm", marginbottom="0.212cm", keepwithnext="always"
            )
            style.addElement(p)
            p = odf.style.TextProperties(
                fontname="Sawasdee",
                fontsize="14pt",
                fontnameasian="DejaVu LGC Sans",
                fontsizeasian="14pt",
                fontnamecomplex="DejaVu LGC Sans",
                fontsizecomplex="14pt",
                fontweight="bold",
                fontweightasian="bold",
                fontweightcomplex="bold",
            )
            style.addElement(p)
            document.styles.addElement(style)

        if self.get_style("Data table") is None:
            style = odf.style.Style(
                name="Data table",
                family="table",
            )
            document.styles.addElement(style)

    def save(self, path: str):
        """Saves document to the specified path.

        Args:
            path (str): _description_
        """
        logging.info("Writing file: %s", path)
        self.odf_document.save(path)

    def append_table(self, name: str, **kw_args):
        """Appends a table to the data dictionary OpenDocument text
        document.

        Args:
            name (str): a name for the table
        """
        self.tables[name] = DocDataTable(document=self, name=name, **kw_args)

    def append_heading(self, text: str, level: int = 1):
        """Appends a heading section containing the text provided,

        Args:
            text (str): contents of the heading,
            level (int): level of the heading. Defaults to 1.
        """
        heading = odf.text.H(stylename="Heading", text=text, outlinelevel=level)
        self.odf_document.text.addElement(heading)

    def append_paragraph(self, text: str):
        """Appends a paragraph of text containing the text provided,

        TODO: Add support for converting markdown text.

        Args:
            text (str): contents of the paragraph
        """
        for line in text.split("\n\n"):
            paragraph = odf.text.P(stylename="Standard", text=line)
            self.odf_document.text.addElement(paragraph)


def create_data_dictionary(
    data_package: Package,
    output: str,
    lang: str = "en",
):
    """Creates a data dictionary text document from a Frictionless Data
    package and table schema.

    Args:
        data_package (Package): a Frictionless data package.
        output (str): path to the resulting odf file.
        lang (str): language code to use for the table column headers.
            Defaults to "en".
    """
    document = DocumentWithTables()

    if data_package.title:
        document.append_heading(data_package.title)
    if data_package.description:
        document.append_paragraph(data_package.description)

    for resource in data_package.resources:
        document.append_table(
            resource.name,
            column_names=DATA_DICT_COLUMN_NAMES[lang],
            title=resource.title,
            description=resource.description,
        )
        rows = []
        for field in resource.schema.fields:
            rows.append([field.name, field.type, field.description])
        document.tables[resource.name].add_rows(rows)

    document.save(output)


def fill_template_table(
    resource: Resource,
    document: OpenDocumentText,
) -> OpenDocumentText:
    """Fills a table in a template document with data from the given
    resource schema.

    Args:
        resource (Resource): a Frictionless Data Resource.
        document (DocumentWithTables): a DocumentWithTables object.

    Returns:
        DocumentWithTables: the document with the table filled out.
    """
    if resource.name not in document.tables.keys():
        raise ValueError(f"Table with id '{resource.name}' not found in document.")
    table = document.tables[resource.name]
    rows = []
    for field in resource.schema.fields:
        rows.append([field.name, field.type, field.description])
    table.add_rows(rows)
    return document


def create_data_dictionary_from_template(
    data_package: Package,
    doc_template: str,
    resource_names: List[str] = None,
    # after_heading: str = None, # TODO
    output: str = None,
):
    """Creates a data dictionary text document from a Frictionless Data
    package and table schema, using a provided document template..

    Args:
        data_package (str): path to the Frictionless data package
            descriptor.
        doc_template (str): path to the odf file to be used as a
            template. If omitted, will create a new document from
            scratch.
        resource_name (List[str]): list of resource names in the data
            package which will update the table.

            Default to None.

            If None, will update all tables whose name match the
            resource names in the data package.
        after_heading (str, optional): Text of the heading under which
            the new table will be created.
            If None, will use the resource title from the data package
            to search the document for headings with a corresponding
            title.

            If None, won't add the resource's title and description.
            Defaults to None.
        output (str): path to the resulting odf file. Defaults to None.
            Caution: if None, will overwrite the original template
            document!
    """
    document = DocumentWithTables.load_from_template(doc_template, load_all_tables=True)

    if resource_names is None:
        resource_names = data_package.resource_names
    for resource_name in resource_names:
        if resource_name in document.tables.keys():
            resource = data_package.get_resource(resource_name)
            document = fill_template_table(resource, document)

    document.save(output)
