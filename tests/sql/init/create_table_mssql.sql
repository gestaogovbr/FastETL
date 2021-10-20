CREATE TABLE {table_name} (
	Name          VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
	Description   TEXT COLLATE SQL_Latin1_General_CP1_CI_AS,
	Description2  VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS,
	Age           BIGINT,
	Weight        FLOAT,
	Birth         DATE,
	Active        BIT,
	date_time     DATETIME2
);

