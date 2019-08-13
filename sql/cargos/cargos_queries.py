query_fetch_meses = """
    SELECT DISTINCT ano_mes
    FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
    ORDER BY ano_mes DESC;
"""

query_estatisticas_cargos = """
WITH table_aggr_mes AS (
	SELECT ano_mes
          ,ano_mes_dt
		  ,orgao_codigo_siorg
		  ,orgao_nome
		  ,orgao_natureza_juridica
		  ,orgao_classificacao
		  ,cargo_descricao
		  ,cargo_tipo
		  ,quantidade_cargos
		  ,data_snapshot
	  FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
	  WHERE ano_mes = %s
	    AND CONVERT(date, data_snapshot) = (SELECT CONVERT(date, MAX(data_snapshot))
									  FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao)
	),

table_aggr_mes_anterior AS (
	SELECT ano_mes
          ,ano_mes_dt
		  ,orgao_codigo_siorg
		  ,orgao_nome
		  ,orgao_natureza_juridica
		  ,orgao_classificacao
		  ,cargo_descricao
		  ,cargo_tipo
		  ,quantidade_cargos
		  ,data_snapshot
	  FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
	  WHERE ano_mes = %s
	    AND CONVERT(date, data_snapshot) = (SELECT CONVERT(date, MAX(data_snapshot))
									  FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao)
	)

INSERT INTO PGG_DW.CONTROLE.cargos_estatisticas (
	ano_mes
    , ano_mes_dt
	, orgao_codigo_siorg
	, orgao_nome
	, orgao_natureza_juridica
	, orgao_classificacao
	, cargo_descricao
	, quantidade_cargos_mes
	, quantidade_cargos_mes_anterior
	, variacao_bruta_cargos
	, variacao_percentual_cargos
	, data_snapshot_origem_mes
	, data_snapshot_origem_mes_anterior
	, data_snapshot
)
SELECT t_mes.ano_mes
       , t_mes.ano_mes_dt
	   , t_mes.orgao_codigo_siorg
	   , t_mes.orgao_nome
	   , t_mes.orgao_natureza_juridica
	   , t_mes.orgao_classificacao
	   , t_mes.cargo_descricao
	   , t_mes.quantidade_cargos AS quantidade_cargos_mes
	   , t_mes_anterior.quantidade_cargos AS quantidade_cargos_mes_anterior
	   , (t_mes.quantidade_cargos - t_mes_anterior.quantidade_cargos) AS variacao_bruta_cargos
	   , (CASE
			WHEN t_mes.quantidade_cargos = 0 AND t_mes_anterior.quantidade_cargos = 0
				THEN 0
			WHEN t_mes.quantidade_cargos = 0 AND t_mes_anterior.quantidade_cargos <> 0
				THEN 1
			ELSE
				(t_mes.quantidade_cargos - t_mes_anterior.quantidade_cargos) / t_mes.quantidade_cargos
		END) AS variacao_percentual_cargos
		, t_mes.data_snapshot AS data_snapshot_origem_mes
		, t_mes_anterior.data_snapshot AS data_snapshot_origem_mes_anterior
		,GETDATE() as data_snapshot
FROM table_aggr_mes t_mes
JOIN table_aggr_mes_anterior t_mes_anterior
	ON t_mes.orgao_codigo_siorg = t_mes_anterior.orgao_codigo_siorg
	   AND t_mes.cargo_descricao = t_mes_anterior.cargo_descricao;"""

query_checa_carga_dag = """
SELECT COUNT(ano_mes) as cnt_ano_mes
FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
WHERE ano_mes = {{ macros.datetime(execution_date.year, execution_date.month -1, 1).strftime("%Y%m") }};
"""
