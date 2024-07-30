# Change log

Here are only the breaking and most significant changes. For a full
account of changes, please see the
[commit history](https://github.com/gestaogovbr/FastETL/commits/main).

## 0.0.35
* Auto-generate Openmetadata lineage

## 0.0.34
* Create test suite for db_connection 

## 0.0.33
* Make DBConnection context manager more flexible

## 0.0.32
* Add timeout to DadosGovBrHook

## 0.0.23
* New estimated max id parameter
* Add psycopg2 execute_batch to destination table at copy_by_key_interval

## 0.0.22
* Add export_file on gdrive (GSheetHook)

## 0.0.15
* Change executemany for execute_batch for postgres for better perfomance

## 0.0.13
* Include yaml config files on package build

## 0.0.12
* Adds functionality to create table on destination when source conn_type is teiid.

## 0.0.9
* Add operator template


## 0.0.7
* Update README.md and add English version
* Create new DadosGovBrHook

## 0.0.4

* Added the tabular data package's title and description to the top of
  the data dictionary file created by
  `TabularDataPackageToDataDictionaryOperator`.
* Added a `DocumentTemplateToDataDictionaryOperator` that allows for
  creating a data dictionary from an ODT document template and a tabular
  data package.
