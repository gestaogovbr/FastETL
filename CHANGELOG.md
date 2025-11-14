# Change log

Here are only the breaking and most significant changes. For a full
account of changes, please see the
[commit history](https://github.com/gestaogovbr/FastETL/commits/main).


## 0.2.8

* Restore support for teiid driver as a source for incremental loads
  ([issue-232](https://github.com/gestaogovbr/FastETL/issues/232))


## 0.2.7

* Rename new module to avoid possible conflict with Python standard
  library ([issue-227](https://github.com/gestaogovbr/FastETL/issues/227))
  https://github.com/gestaogovbr/FastETL/pull/229
* Refactor query replication tests for performance
  ([issue-228](https://github.com/gestaogovbr/FastETL/issues/228))


## 0.2.6

* Implement support for Jinja templates and template files in source
  query argument of `DbToDbOperator`
  ([issue-225](https://github.com/gestaogovbr/FastETL/issues/225))

Note: skipped version numbers 0.2.4 and 0.2.5.


## 0.2.3

* Add a flag in DbtoDbOperator whether or not to attempt to create table
  at the destination
  ([issue-219](https://github.com/gestaogovbr/FastETL/issues/219))
* Remove any surrounding whitespace from query before fetching first row
  ([issue-217](https://github.com/gestaogovbr/FastETL/issues/217))
* Use white space as a separator in datetimes instead of `T`
  (for compatibility with teiid,
  [issue-215](https://github.com/gestaogovbr/FastETL/issues/215))


## 0.2.2

* Fix bug on Teiid when formatting date to isoformat
  ([issue-215](https://github.com/gestaogovbr/FastETL/issues/215))


## 0.2.1
* Fix bug error when creating table with CTE statement
  ([issue-213](https://github.com/gestaogovbr/FastETL/issues/213))


## 0.2.0

* Removed module bacen_STA_hook


## 0.1.0

* In development environment using docker compose, install fastetl
  as an editable package with a mounted volume, so edited code has
  an effect when running tests
* Add support for 'until_datetime' in incremental data sync
* Reorder arguments to sync_db_2_db: date_column is optional


## 0.0.43
* Update DbtoDbOperator to create destination table from a query source

## 0.0.40
* Update openmetadata-ingestion lib. It was affecting pydantic version.

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
