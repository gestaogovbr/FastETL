UPDATE PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
SET cargo_tipo = (CASE
					WHEN SUBSTRING(cargo_descricao, 1, 3) like '[Aa-Zz][Aa-Zz][Aa-Zz]'
						THEN SUBSTRING(cargo_descricao, 1, 3)
					ELSE
						SUBSTRING(cargo_descricao, 1, 2)
					END)
FROM PGG_DW.CONTROLE.qt_cargos_orgao_classificacao
