INSERT INTO PGG_DW.CONTROLE.cargos_registro (
    ano_mes,
    ano_mes_dt,
    orgao_unificado_id,
    orgao_nome,
    qt_registros,
    data_snapshot
)
SELECT DISTINCT
  LEFT(TEMPO_DIA_ID, 6) AS ano_mes,
-- CONVERT(datetime, CAST(TEMPO_DIA_ID AS CHAR(8))) AS ano_mes_dt,
  CONVERT(datetime, LEFT(TEMPO_DIA_ID, 6) + '01') AS ano_mes_dt,
  FT.ORGAO_UNIFICADO_ID as orgao_unificado_id,
  U.ORGAO_UNIFICADO_NOME as orgao_nome,
  COUNT(1) AS qt_registros,
  GETDATE() as data_snapshot
FROM
PGG_DW.DW_APF_FATOS.FT_INFORMACOES_SIAPE FT
  JOIN PGG_DW.DW_APF_GERAL.DM_ORGAO_UNIFICADO U
    ON FT.ORGAO_UNIFICADO_ID = U.ORGAO_UNIFICADO_ID
WHERE INFORMACAO_TIPO_ID = 20001
GROUP BY TEMPO_DIA_ID, FT.ORGAO_UNIFICADO_ID, U.ORGAO_UNIFICADO_NOME;
