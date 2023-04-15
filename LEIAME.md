![Logotipo do FastETL. É um canivete tipo suiço com várias peças abertas](docs/images/logo.svg)

<p align="center">
    <em>Framework fastETL, moderno, versátil, faz quase tudo.</em>
</p>

This text is also available in English: 🇬🇧[README.md](README.md).

---

[![CI Tests](https://github.com/economiagovbr/FastETL/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/economiagovbr/FastETL/actions/workflows/ci-tests.yml)

O **FastETL** é um pacote de plugins do Airflow para construção de pipelines de dados para uma variedade de cenários comuns.

Principais funcionalidades:
* **Replicação** de tabelas *full* ou incremental em bancos de dados SQL
  Server, Postgres e MySQL
* Carga de dados a partir do **GSheets** e de planilhas na rede **Samba/Windows**
* Extração de **CSV** a partir do SQL
* Limpeza de dados usando tasks customizadas de *patch* de dados (ex.:
  para coordenadas geográficas sujas, mapear valores canônicos em colunas,
  etc.)
* Consulta à API do Diário Oficial da União (**DOU**)
* Usar um serviço [Open Street Routing Machine](https://project-osrm.org/)
  para calcular distâncias de rotas
* Usar a API do [CKAN](https://docs.ckan.org/en/2.10/api/index.html) ou
  do dados.gov.br para atualizar os metadados de um dataset
* Usar Frictionless
  [Tabular Data Packages](https://specs.frictionlessdata.io/tabular-data-package/)
  para escrever dicionários de dados no formato OpenDocument Text

<!-- Contar a história da origem do FastETL -->
Este framework é mantido por uma rede de desenvolvedores de diversas
equipes do Ministério da Gestão e da Inovação em Serviços Públicos e é o
resultado acumulado da utilização do
[Apache Airflow](https://airflow.apache.org/), uma ferramenta livre de
código aberto, a partir de 2019.

**Para governo:** O fastETL é utilizado largamente para replicação de dados acessados via Quartzo (DaaS) do Serpro.

# Instalação no Airflow

O FastETL implementa os padrões de plugins do Airflow e para ser
instalado, simplesmente adicione o pacote
`apache-airflow-providers-fastetl` às suas dependências Python em seu
ambiente Airflow.

Ou instale-o com

```bash
pip install apache-airflow-providers-fastetl
```

Para ver um exemplo de container do Apache Airflow que usa o FastETL,
confira o repositório
[airflow2-docker](https://github.com/economiagovbr/airflow2-docker).

# Testes

A suíte de testes usa contêineres Docker para simular um ambiente
completo de uso, inclusive com o Airflow e os bancos de dados. Por isso,
para executar os testes, é necessário primeiro instalar o Docker e o
docker-compose.

Para quem usa Ubuntu 20.04, basta digitar no terminal:

```bash
snap install docker
```

Para outras versões e sistemas operacionais, consulte a
[documentação oficial do Docker](https://docs.docker.com/get-docker/).


Para construir os contêineres:

```bash
make setup
```

Para rodar os testes execute:

```bash
make setup && make tests
```

Para desmontar o ambiente execute:

```bash
make down
```

# Exemplo de uso

A principal funcionalidade do FastETL é o operador
`DbToDbOperator`. Ele copia dados entre os bancos `postgres` e
`mssql`. O MySQL também é suportado como fonte.

Aqui um exemplo:

```python
from datetime import datetime
from airflow import DAG
from fastetl.operators.db_to_db_operator import DbToDbOperator

default_args = {
    "start_date": datetime(2023, 4, 1),
}

dag = DAG(
    "copy_db_to_db_example",
    default_args=default_args,
    schedule_interval=None,
)


t0 = DbToDbOperator(
    task_id="copy_data",
    source={
        "conn_id": airflow_source_conn_id,
        "schema": source_schema,
        "table": table_name,
    },
    destination={
        "conn_id": airflow_dest_conn_id,
        "schema": dest_schema,
        "table": table_name,
    },
    destination_truncate=True,
    copy_table_comments=True,
    chunksize=10000,
    dag=dag,
)
```

Mais detalhes sobre os parâmetros e funcionamento do `DbToDbOperator`
nos arquivos:

* [fast_etl.py](fastetl/custom_functions/fast_etl.py)
* [db_to_db_operator.py](fastetl/operators/db_to_db_operator.py)

# Como colaborar

A escrever no documento `CONTRIBUTING.md` (issue
[#4](/economiagovbr/FastETL/issues/4)).
