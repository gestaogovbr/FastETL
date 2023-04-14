from importlib.metadata import version
import os

if not os.getenv('FASTETL_DEV'):
    __name__ = "apache-airflow-providers-fastetl"
    __version__ = version(__name__)

    # This is needed to allow Airflow to pick up specific metadata fields
    # it needs for certain features.

    def get_provider_info():
        return {
            "package-name": __name__,
            "name": "FastETL Apache Airflow Provider",
            "description": "Copy data between Db Tables.",
            "versions": [__version__],
        }