query_fetch_meses = """
    SELECT DISTINCT ano_mes
    FROM PGG_DW.CONTROLE.pessoal_orgao
    ORDER BY ano_mes DESC;
"""

query_estatistica_orgao = """
WITH table_aggr_mes AS (
	SELECT 
		ano_mes
		, ano_mes_dt
		, informacao_tipo_id
		, informacao_tipo_desc
		, orgao_codigo_siorg
		, orgao_nome
		, orgao_natureza_juridica
		, orgao_classificacao
		, sum_valor
		, data_snapshot
	  FROM PGG_DW.CONTROLE.pessoal_orgao
	  WHERE ano_mes = %s),

table_aggr_mes_anterior AS (
	SELECT
		ano_mes
		, ano_mes_dt
		, informacao_tipo_id
		, informacao_tipo_desc
		, orgao_codigo_siorg
		, orgao_nome
		, orgao_natureza_juridica
		, orgao_classificacao
		, sum_valor
		, data_snapshot
	  FROM PGG_DW.CONTROLE.pessoal_orgao
	  WHERE ano_mes = %s)
INSERT INTO PGG_DW.CONTROLE.pessoal_estatistica (
		ano_mes
		, ano_mes_dt
		, orgao_codigo_siorg
		, orgao_nome
		, orgao_natureza_juridica
		, orgao_classificacao
		, sum_valor_mes
		, sum_valor_mes_anterior
		, variacao_bruta_valor
		, variacao_bruta_valor_abs
		, variacao_percentual_valor
		, data_snapshot)
SELECT t_mes.ano_mes
       ,t_mes.ano_mes_dt
	   ,t_mes.orgao_codigo_siorg
	   ,t_mes.orgao_nome
	   ,t_mes.orgao_natureza_juridica
	   ,t_mes.orgao_classificacao
	   ,t_mes.sum_valor AS sum_valor_mes
	   ,t_mes_anterior.sum_valor AS sum_valor_mes_anterior
	   ,(t_mes.sum_valor - t_mes_anterior.sum_valor) AS variacao_bruta_valor
	   ,abs(t_mes.sum_valor - t_mes_anterior.sum_valor) AS variacao_bruta_valor_abs
	   ,(CASE
			WHEN t_mes.sum_valor = 0 AND t_mes_anterior.sum_valor = 0
				THEN 0
			WHEN t_mes.sum_valor = 0 AND t_mes_anterior.sum_valor <> 0
				THEN 1
			ELSE
				(t_mes.sum_valor - t_mes_anterior.sum_valor) / t_mes.sum_valor
		END) AS variacao_percentual_valor
		,GETDATE() as data_snapshot
INTO PGG_DW.CONTROLE.pessoal_estatistica
FROM table_aggr_mes t_mes
JOIN table_aggr_mes_anterior t_mes_anterior
	ON t_mes.orgao_codigo_siorg = t_mes_anterior.orgao_codigo_siorg
	   AND t_mes.informacao_tipo_id = t_mes_anterior.informacao_tipo_id;
"""

query_checa_carga_dag = """
SELECT COUNT(ano_mes) as cnt_ano_mes
FROM PGG_DW.CONTROLE.pessoal_orgao
WHERE ano_mes = {{ macros.datetime(execution_date.year, execution_date.month -1, 1).strftime("%Y%m") }};
"""
