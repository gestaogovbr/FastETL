INSERT INTO PGG_DW.CONTROLE.pessoal_registro (
    ano_mes
    , ano_mes_dt
    , informacao_tipo_id
    , informacao_tipo_desc
    , orgao_unificado_id
    , orgao_nome
    , qt_registros
    , data_snapshot
)
SELECT DISTINCT
	LEFT(TEMPO_DIA_ID, 6) AS ano_mes
	-- CONVERT(datetime, CAST(TEMPO_DIA_ID AS CHAR(8))) AS ano_mes_dt,
	, CONVERT(datetime, LEFT(TEMPO_DIA_ID, 6) + '01') AS ano_mes_dt
	, FT.INFORMACAO_TIPO_ID AS informacao_tipo_id
	, I.INFORMACAO_TIPO_DESCRICAO AS informacao_tipo_desc
	, FT.ORGAO_UNIFICADO_ID as orgao_unificado_id
	, U.ORGAO_UNIFICADO_NOME as orgao_nome
	, COUNT(1) AS qt_registros
	, GETDATE() as data_snapshot
FROM
PGG_DW.DW_APF_FATOS.FT_INFORMACOES_SIAPE FT
JOIN PGG_DW.DW_APF_GERAL.DM_ORGAO_UNIFICADO U
	ON FT.ORGAO_UNIFICADO_ID = U.ORGAO_UNIFICADO_ID
JOIN PGG_DW.DW_APF_GERAL.DM_INFORMACAO_TIPO I
	ON FT.INFORMACAO_TIPO_ID = I.INFORMACAO_TIPO_ID
WHERE FT.INFORMACAO_TIPO_ID = {{ params.informacao_tipo_id }}
GROUP BY
	TEMPO_DIA_ID
	, FT.INFORMACAO_TIPO_ID
	, I.INFORMACAO_TIPO_DESCRICAO
	, FT.ORGAO_UNIFICADO_ID
	, U.ORGAO_UNIFICADO_NOME;
