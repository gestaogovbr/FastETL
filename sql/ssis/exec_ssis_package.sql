-- Executa uma package SSIS

DECLARE @execution_id BIGINT

-- Create execution
EXEC [SSISDB].[catalog].[create_execution] 
	@package_name = {{ params.package_name }}
	, @project_name = {{ params.project_name }}
	, @folder_name = {{ params.folder_name }}
	, @use32bitruntime = False
	, @reference_id = NULL
	, @execution_id = @execution_id OUTPUT

-- System parameters
EXEC [SSISDB].[catalog].[set_execution_parameter_value] 
	@execution_id
	, @object_type = 50	    -- System parameter
	, @parameter_name = 'SYNCHRONIZED'
	, @parameter_value = 0	-- Precisa ser "not synchronized" para executar pelo Airflow

-- Execute the package
EXEC [SSISDB].[catalog].[start_execution] @execution_id

