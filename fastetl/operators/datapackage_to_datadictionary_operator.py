"""Airflow operator to generate a data dictionary in Open Document Text
format from a
[Tabular Data Package](https://specs.frictionlessdata.io/tabular-data-package/)
descriptor containing table schema.
"""
from typing import Sequence
import logging

from airflow.models.baseoperator import BaseOperator

from FastETL.custom_functions.utils.odf_tables import create_data_dictionary


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

    data_package (str): path to the Frictionless data package descriptor.
    output_document (str): path to the resulting odf file.
    lang (str): language code to use for the table column headers.
        Defaults to "en".
    """
    template_fields: Sequence[str] = ("data_package", "output_document", "lang", )
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
        create_data_dictionary(self.data_package, self.output_document, self.lang)
