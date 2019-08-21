SELECT	e1.execution_id, 
		CASE e1.status
			WHEN 1 THEN 'criado'
			WHEN 2 THEN 'executando'
			WHEN 3 THEN 'cancelado'
			WHEN 4 THEN 'falha'
			WHEN 5 THEN 'pendente'
			WHEN 6 THEN 'encerrado inesperadamente'
			WHEN 7 THEN 'sucesso'
			WHEN 8 THEN 'parando'
			WHEN 9 THEN 'concluido'
			ELSE 'desconhecido'
		END AS status, 
		e1.start_time, 
		e1.end_time, 
		e1.cpu_count
FROM [SSISDB].[catalog].[executions] e1
INNER JOIN
	(SELECT MAX(execution_id) AS max_exec_id
	 FROM [SSISDB].[catalog].[executions]
	 WHERE project_name = 'prjExtComprasDiretasScdp'
	   AND package_name = 'teste_exporta_csv.dtsx'
	) e2
ON e1.execution_id = e2.max_exec_id ;
