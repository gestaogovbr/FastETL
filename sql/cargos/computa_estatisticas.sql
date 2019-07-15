WITH table_aggr_mes AS (
	SELECT ano_mes
		  ,orgao_codigo_siorg
		  ,orgao_nome
		  ,orgao_natureza_juridica
		  ,orgao_classificacao
		  ,cargo_descricao
		  ,cargo_tipo
		  ,quantidade_cargos
		  ,data_snapshot
	  FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
	  WHERE ano_mes = %s),

table_aggr_mes_anterior AS (
	SELECT ano_mes
		  ,orgao_codigo_siorg
		  ,orgao_nome
		  ,orgao_natureza_juridica
		  ,orgao_classificacao
		  ,cargo_descricao
		  ,cargo_tipo
		  ,quantidade_cargos
		  ,data_snapshot
	  FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
	  WHERE ano_mes = %s)

SELECT t_mes.ano_mes,
	   t_mes.orgao_codigo_siorg,
	   t_mes.orgao_nome,
	   t_mes.orgao_natureza_juridica,
	   t_mes.orgao_classificacao,
	   t_mes.cargo_descricao,
	   t_mes.quantidade_cargos AS quantidade_cargos_mes,
	   t_mes_anterior.quantidade_cargos AS quantidade_cargos_mes_anterior,
	   (t_mes.quantidade_cargos - t_mes_anterior.quantidade_cargos) AS variacao_bruta_cargos,
	   (CASE
			WHEN t_mes.quantidade_cargos = 0 AND t_mes_anterior.quantidade_cargos = 0
				THEN 0
			WHEN t_mes.quantidade_cargos = 0 AND t_mes_anterior.quantidade_cargos <> 0
				THEN 1
			ELSE
				(t_mes.quantidade_cargos - t_mes_anterior.quantidade_cargos) / t_mes.quantidade_cargos
		END) AS variacao_percentual_cargos
FROM table_aggr_mes t_mes
JOIN table_aggr_mes_anterior t_mes_anterior
	ON t_mes.orgao_codigo_siorg = t_mes_anterior.orgao_codigo_siorg
	   AND t_mes.cargo_descricao = t_mes_anterior.cargo_descricao
