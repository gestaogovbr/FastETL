"""Airflow operator to generate a data dictionary in Open Document Text
format from a
[Tabular Data Package](https://specs.frictionlessdata.io/tabular-data-package/)
descriptor containing table schema.
"""
from typing import Sequence
import logging

from frictionless import Package

from airflow.models.baseoperator import BaseOperator

from fastetl.custom_functions.utils.odf_tables import (
    create_data_dictionary,
    create_data_dictionary_from_template,
)


class TabularDataPackageToDataDictionaryOperator(BaseOperator):
    """
    Creates a data dictionary document, in Open Document Text format,
    describing the tables present in a Tabular Data Package.

    For the Tabular Data Package specifications, see:
    https://specs.frictionlessdata.io/tabular-data-package/

    Each table created will have three columns:
    * field name
    * type
    * description

    Args:
        data_package (str): path to the Frictionless data package descriptor.
        output_document (str): path to the resulting odf file.
        lang (str): language code to use for the table column headers.
            Defaults to "en".
    """

    template_fields: Sequence[str] = (
        "data_package",
        "output_document",
        "lang",
    )
    ui_color = "#efdec2"

    def __init__(
        self,
        data_package: str,
        output_document: str,
        lang: str = "en",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_package = data_package
        self.output_document = output_document
        self.lang = lang

    def execute(self, context: dict):
        """Execute the operator."""
        logging.info("Data package: %s", self.data_package)
        logging.info("Output document: %s", self.output_document)
        logging.info("Language: %s", self.lang)
        create_data_dictionary(
            data_package=Package(self.data_package),
            output=self.output_document,
            lang=self.lang,
        )


class DocumentTemplateToDataDictionaryOperator(BaseOperator):
    """
    Creates a data dictionary document, in Open Document Text format,
    by taking a document template and updating the tables that match
    with existing resource names.

    The table name in the document must match exactly with existing
    resource names in the Tabular Data Package provided.

    For the Tabular Data Package specifications, see:
    https://specs.frictionlessdata.io/tabular-data-package/

    Each table in the template must have three columns:
    * field name
    * type
    * description

    Args:
        data_package (str): path to the Frictionless data package descriptor.
        document_template(str): path to the OpenDocument format document
            to be used as a template.
        output_document (str): path to the resulting odf file.
    """

    template_fields: Sequence[str] = (
        "data_package",
        "document_template",
        "output_document",
    )
    ui_color = "#e2efc2"

    def __init__(
        self,
        data_package: str,
        document_template: str,
        output_document: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_package = data_package
        self.document_template = document_template
        self.output_document = output_document

    def execute(self, context: dict):
        """Execute the operator."""
        logging.info("Data package: %s", self.data_package)
        logging.info("Template document: %s", self.document_template)
        logging.info("Output document: %s", self.output_document)
        create_data_dictionary_from_template(
            data_package=Package(self.data_package),
            doc_template=self.document_template,
            output=self.output_document,
        )
