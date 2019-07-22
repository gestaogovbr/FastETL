query_checa_atualizacao_2 = """
    SELECT COUNT(1)
    FROM PGG_DW.DW_APF_FATOS.FT_INFORMACOES_SIAPE
    WHERE INFORMACAO_TIPO_ID = 20001
        AND SUBSTRING(CONVERT(VARCHAR, TEMPO_DIA_ID), 1, 6) = '{{ macros.ds_format(macros.ds_add(ds, -25), "%Y-%m-%d", "%Y%m") }}';
"""
query_checa_atualizacao = """
    SELECT *
FROM PGG_DW.CONTROLE.sensor_test
WHERE ID = %s;
"""
