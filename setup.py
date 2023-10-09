"""
Setup.py for FastETL Airflow provider package.
"""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

__version__ = "0.0.26"

"""Perform the package apache-airflow-providers-fastetl setup."""
setup(
    name="apache-airflow-providers-fastetl",
    version=__version__,
    description="FastETL custom package Apache Airflow provider.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={"apache_airflow_provider": ["provider_info=fastetl.__init__:get_provider_info"]},
    license="Apache License 2.0",
    packages=find_packages(exclude=["*tests.*", "*tests"]),
    package_data={'': ['*.yml']},
    include_package_data=True,
    install_requires=[
        "apache-airflow>=2.3",
        "apache-airflow-providers-microsoft-mssql",
        "apache-airflow-providers-mysql",
        "apache-airflow-providers-postgres",
        "apache-airflow-providers-common-sql",
        "alembic>=1.8.1",
        "beautifulsoup4>=4.1.11",
        "ckanapi>=4.6",
        "frictionless>=5.11.1",
        "markdown>=3.4.1",
        "odfpy>=1.4.1",
        "pandas>=1.5.2,<2",
        "psycopg2>=2.9.5",
        "pygsheets>=2.0.5",
        "pyodbc>=4.0.35",
        "pysmb>=1.2.6",
        "python-slugify>=7.0.0",
        "pytz>=2022.6",
        "requests>=2.28.1",
        "SQLAlchemy>=1.4.44",
        "PyYAML==6.0"
    ],
    setup_requires=["setuptools", "wheel"],
    author="Time de Dados CGINF",
    author_email="seges.cginf@economia.gov.br",
    url="https://github.com/gestaogovbr/FastETL",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.8",
)
