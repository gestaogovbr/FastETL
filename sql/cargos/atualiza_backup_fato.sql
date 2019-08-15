INSERT INTO PGG_DW.DW_APF_FATOS_BKP.FT_INFORMACOES_SIAPE_BKP (
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
)
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
WHERE INFORMACAO_TIPO_ID = 20001 AND SUBSTRING(CONVERT(VARCHAR, TEMPO_DIA_ID), 1, 6) <> {{ macros.datetime(execution_date.year, execution_date.month -1, 1).strftime("%Y%m") }};
