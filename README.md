O FastETL é um pacote de plugins do Airflow para construção de pipelines de dados para uma variedade de cenários comuns.

Principais funcionalidades:
* **Replicação** de tabelas *full* ou incremental em bancos de dados SQL Server e Postgres
* Carga de dados a partir do **GSheets** e de planilhas na rede **Samba/Windows**
* Extração de **CSV** a partir do SQL
* Monitoramento do **DOU** com relatório por email -- NOVO --

<!-- Contar a história da origem do FastETL -->
Este framework é mantido por uma rede de desenvolvedores de diversas equipes do Ministério da Economia e é o resultado acumulado da utilização do [Apache Airflow](https://airflow.apache.org/), uma ferramenta livre de código aberto, a partir de 2019.

# Instalação no Airflow

O FastETL implementa os padrões de plugins do Airflow e para ser instalado basta que ele seja copiado para o diretório `plugins` no ambiente da instalação do Airflow. Conheça e utilize também nosso ambiente airflow local (o [airflow-docker-local](https://github.com/economiagovbr/airflow-docker-local/)) que já possui o FastETL integrado.

# Exemplo de uso

# Como colaborar