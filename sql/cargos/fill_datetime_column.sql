UPDATE PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
SET ano_mes_dt = CAST(
                  SUBSTRING(CAST(ano_mes AS VARCHAR(6)), 1, 4) +
                  SUBSTRING(CAST(ano_mes AS VARCHAR(6)), 5, 6) +
                  '01'
                  AS DATETIME)
FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
