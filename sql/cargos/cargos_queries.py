query_fetch_meses = """
    SELECT DISTINCT ano_mes
    FROM PGG_DW.CONTROLE.cargos_orgao
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
	  FROM PGG_DW.CONTROLE.cargos_orgao
	  WHERE ano_mes = %s
	    AND CONVERT(date, data_snapshot) = (SELECT CONVERT(date, MAX(data_snapshot))
									  FROM PGG_DW.CONTROLE.cargos_orgao)
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
	  FROM PGG_DW.CONTROLE.cargos_orgao
	  WHERE ano_mes = %s
	    AND CONVERT(date, data_snapshot) = (SELECT CONVERT(date, MAX(data_snapshot))
									  FROM PGG_DW.CONTROLE.cargos_orgao)
	)

INSERT INTO PGG_DW.CONTROLE.cargos_estatistica (
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
	, variacao_bruta_cargos_abs
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
	   , ABS(t_mes.quantidade_cargos - t_mes_anterior.quantidade_cargos) AS variacao_bruta_cargos_abs
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
FROM PGG_DW.CONTROLE.cargos_orgao
WHERE ano_mes = {{ macros.datetime(execution_date.year, execution_date.month -1, 1).strftime("%Y%m") }};
"""

query_compara_fato_stage = """
WITH tbl_fato AS (
	SELECT
		INFORMACOES_ID
		, HARMONIZACAO_BASICA_ID
		, INFORMACAO_TIPO_ID
		, TEMPO_DIA_ID
		, ORGAO_UNIFICADO_ID
		, ORGAO_SUPERIOR_UNIFICADO_ID
		, VINCULO_SERV
		, COR_ORIGEM_ETNICA
		, REGIME_JURIDICO
		, SITUACAO_VINC_SERV
		, IDADE
		, FAIXA_ETARIA4
		, ABONO_PERMANENCIA_ID
		, ESCOLARIDADE
		, SEXO
		, CARGO_EFT_NIVEL
		, MUNICIPIO_ID
		, CARGO_COMISS_SUBNIVEL_ID
		, PESSOA_TIPO_VINCULO_ID
		, VALOR
		, N5_ID_UNIDADE_ORGANIZACIONAL
		, DENOMINACAO_CARGO_ID
		, UORG_VINC
		, FT_INFORMACOES_SIAPE_DT_INCLUSAO
	FROM PGG_DW.DW_APF_FATOS.FT_INFORMACOES_SIAPE
	WHERE INFORMACAO_TIPO_ID = 20001 AND SUBSTRING(CONVERT(VARCHAR, TEMPO_DIA_ID), 1, 6) <> {{ macros.datetime(execution_date.year, execution_date.month -1, 1).strftime("%Y%m") }}
),
tbl_fato_bkp AS (
	SELECT
		INFORMACOES_ID
		, HARMONIZACAO_BASICA_ID
		, INFORMACAO_TIPO_ID
		, TEMPO_DIA_ID
		, ORGAO_UNIFICADO_ID
		, ORGAO_SUPERIOR_UNIFICADO_ID
		, VINCULO_SERV
		, COR_ORIGEM_ETNICA
		, REGIME_JURIDICO
		, SITUACAO_VINC_SERV
		, IDADE
		, FAIXA_ETARIA4
		, ABONO_PERMANENCIA_ID
		, ESCOLARIDADE
		, SEXO
		, CARGO_EFT_NIVEL
		, MUNICIPIO_ID
		, CARGO_COMISS_SUBNIVEL_ID
		, PESSOA_TIPO_VINCULO_ID
		, VALOR
		, N5_ID_UNIDADE_ORGANIZACIONAL
		, DENOMINACAO_CARGO_ID
		, UORG_VINC
		, FT_INFORMACOES_SIAPE_DT_INCLUSAO
	FROM PGG_DW.DW_APF_FATOS.FT_INFORMACOES_SIAPE_BKP
	WHERE INFORMACAO_TIPO_ID = 20001
)
SELECT COUNT(1) as qt_registros
FROM
	((SELECT *
	FROM tbl_fato)

	EXCEPT

	(SELECT *
	FROM tbl_fato_bkp)) T1
"""
