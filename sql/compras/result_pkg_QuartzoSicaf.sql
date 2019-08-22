
SELECT	COUNT(*)
FROM [SSISDB].[catalog].[executions] e1
INNER JOIN
	(SELECT MAX(execution_id) AS max_exec_id
	 FROM [SSISDB].[catalog].[executions]
	 WHERE project_name = 'PrjQuartzoSicaf'
	   AND package_name = 'carrega Stage.dtsx'
	   AND end_time IS NOT NULL
	) e2
ON e1.execution_id = e2.max_exec_id
WHERE e1.status = 7 ;

