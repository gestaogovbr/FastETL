-- Executa uma package SSIS

DECLARE @execution_id BIGINT

-- Create execution
EXEC [SSISDB].[catalog].[create_execution] 
	@package_name = N'teste_exporta_csv.dtsx'
	, @project_name = N'prjExtComprasDiretasScdp'
	, @folder_name = N'icSCDP'
	, @use32bitruntime = False
	, @reference_id = NULL
	, @execution_id = @execution_id OUTPUT

-- System parameters
EXEC [SSISDB].[catalog].[set_execution_parameter_value] 
	@execution_id
	, @object_type = 50						-- System parameter
	, @parameter_name = 'SYNCHRONIZED'
	, @parameter_value = 0

-- Execute the package
EXEC [SSISDB].[catalog].[start_execution] @execution_id

