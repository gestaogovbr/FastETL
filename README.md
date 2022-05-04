![Logotipo do FastETL. É um canivete tipo suiço com várias peças abertas](docs/logo.png)

<p align="center">
    <em>Framework fastETL, moderno, versátil, faz quase tudo.</em>
</p>

---

[![CI Tests](https://github.com/economiagovbr/FastETL/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/economiagovbr/FastETL/actions/workflows/ci-tests.yml)

O **FastETL** é um pacote de plugins do Airflow para construção de pipelines de dados para uma variedade de cenários comuns.

Principais funcionalidades:
* **Replicação** de tabelas *full* ou incremental em bancos de dados SQL Server e Postgres
* Carga de dados a partir do **GSheets** e de planilhas na rede **Samba/Windows**
* Extração de **CSV** a partir do SQL
* Consulta à API do **DOU**

<!-- Contar a história da origem do FastETL -->
Este framework é mantido por uma rede de desenvolvedores de diversas equipes do Ministério da Economia e é o resultado acumulado da utilização do [Apache Airflow](https://airflow.apache.org/), uma ferramenta livre de código aberto, a partir de 2019.

**Para governo:** O fastETL é utilizado largamente para replicação de dados acessados via Quartzo (DaaS) do Serpro.

# Instalação no Airflow

O FastETL implementa os padrões de plugins do Airflow e para ser
instalado basta que ele seja copiado para o diretório `plugins` no
ambiente da instalação do Airflow.

Atualmente o FastETL depende do nosso ambiente do Airflow com Docker
definido no repositório
[airflow2-docker](https://github.com/economiagovbr/airflow2-docker).
Caso utilize esse ambiente, o FastETl já vem integrado.

No futuro pretendemos transformá-lo em um plugin independente de um
ambiente específico, contendo instruções para instalado em qualquer
ambiente. O primeiro passo para isso será
[documentar as suas dependências](https://github.com/economiagovbr/FastETL/issues/5).

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

# Como colaborar
