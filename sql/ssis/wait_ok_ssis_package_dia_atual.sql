-- Verifica se uma package executou com sucesso (status 7) no dia corrente
-- Para usar com o SensorOperator

--########################################################################
-- ATENCAO!!!
-- Caso a package nao finalize com sucesso, o SensorOperator vai ficar
-- esperando ate dar erro pelo timeout!!!
--########################################################################

-- Espera 30 segundos preventivamente, para nao confundir com uma execucao triggered antes da atual
WAITFOR DELAY '00:00:30'

-- Verifica se ultima execucao da package no dia atual concluiu com sucesso
SELECT COUNT(*)
FROM [SSISDB].[catalog].[executions] e1
INNER JOIN
	(SELECT MAX(execution_id) AS max_exec_id
	 FROM [SSISDB].[catalog].[executions]
	 WHERE project_name = N'{{ params.project_name }}'
	   AND package_name = N'{{ params.package_name }}'
	   AND end_time IS NOT NULL
	) e2
ON e1.execution_id = e2.max_exec_id
WHERE CONVERT(CHAR(8),e1.end_time,112) = CONVERT(CHAR(8),CURRENT_TIMESTAMP,112)
AND e1.status = 7 

