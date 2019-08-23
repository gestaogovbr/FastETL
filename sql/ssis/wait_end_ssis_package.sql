-- Verifica se a execução mais recente da package já finalizou (para usar no Sensor)

SELECT	COUNT(*)
FROM [SSISDB].[catalog].[executions] e1
INNER JOIN
	(SELECT MAX(execution_id) AS max_exec_id
	 FROM [SSISDB].[catalog].[executions]
	 WHERE project_name = '{{ params.project_name }}'
	   AND package_name = '{{ params.package_name }}'
	) e2
ON e1.execution_id = e2.max_exec_id
WHERE e1.end_time IS NOT NULL
