"""
Script de ingestão para a camada Bronze do projeto camara_brasil.

Este módulo define funções para baixar dados da API de Dados Abertos da
Câmara dos Deputados e persistir os arquivos brutos em formato Delta
(ou Parquet) no armazenamento do Databricks.  A camada bronze é a
responsável por armazenar os dados exatamente como foram recebidos da
fonte, garantindo que nenhuma transformação destrutiva seja aplicada
antes da persistência.

As funções estão organizadas por domínio: frentes parlamentares,
deputados, eventos, votações, despesas, CPIs e quaisquer outros
conjuntos que venham a ser necessários.  Cada função retorna um
DataFrame do PySpark contendo os registros recebidos e grava uma
tabela bronze no caminho especificado.  A função `ingest_all` orquestra
a execução de todas as ingestões.

Para executar este script no Databricks Free Edition:

1. Crie um cluster com versão Spark 3.5 ou superior.
2. Importe este arquivo como um notebook ou utilize o menu "Upload Data".
3. Ajuste as variáveis de `BRONZE_PATH` para apontar para o DBFS
   (por exemplo, `dbfs:/mnt/bronze/camara_brasil`).
4. Execute `ingest_all(spark)` em uma célula de notebook.

Observação: As chamadas à API utilizam paginação quando disponível.
Quando um endpoint não fornece paginação, os dados completos são
carregados de uma vez.  Consulte a documentação de cada endpoint para
alterar parâmetros de filtragem (por exemplo, legislatura ou datas).
"""

import json
import math
import os
from typing import Dict, Iterable, List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


# Configurações globais
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
# Caminho padrão para gravação dos dados brutos. Ajuste conforme o ambiente.
BRONZE_PATH = "dbfs:/mnt/bronze/camara_brasil"


def get_api_data(endpoint: str, params: Optional[Dict[str, str]] = None) -> List[Dict]:
    """Realiza chamadas HTTP à API do Dados Abertos com paginação.

    A API da Câmara retorna uma estrutura contendo as chaves `dados`
    e `links`.  Quando o retorno for paginado, os links incluirão uma
    referência para a próxima página (`rel='next'`).  Este método segue
    esses links até consumir todos os registros.

    Args:
        endpoint: Caminho do recurso (ex.: "frentes").
        params: Dicionário de parâmetros de query a serem enviados.

    Returns:
        Lista de objetos Python equivalentes ao conteúdo de `dados`.
    """
    if params is None:
        params = {}

    url = f"{BASE_URL}/{endpoint}"
    results: List[Dict] = []
    while url:
        response = requests.get(url, params=params)
        response.raise_for_status()
        payload = response.json()
        results.extend(payload.get("dados", []))
        # procura link para a próxima página
        links = payload.get("links", [])
        next_link = None
        for link in links:
            if link.get("rel") == "next":
                next_link = link.get("href")
                break
        # após a primeira chamada, os parâmetros já estão embutidos na URL
        url = next_link
        params = None
    return results


def save_bronze(df: DataFrame, table_name: str, spark: SparkSession) -> None:
    """Grava um DataFrame no formato Delta no caminho bronze.

    Se o diretório alvo já existir, ele será sobrescrito.  A
    persistência utiliza o modo "overwrite" para garantir a
    reprocessamento completo na ingestão.

    Args:
        df: DataFrame a ser gravado.
        table_name: Nome lógico da tabela (subdiretório dentro da camada bronze).
        spark: SparkSession ativa.
    """
    path = os.path.join(BRONZE_PATH, table_name)
    (df
     .write
     .format("delta")
     .mode("overwrite")
     .save(path))


def ingest_frentes(spark: SparkSession) -> DataFrame:
    """Ingesta a lista de frentes parlamentares.

    Retorna um DataFrame com os campos básicos disponíveis na API v2:
    id, uri, titulo, dataCriacao, idLegislatura, telefone, situacao,
    urlDocumento, além do coordenador aninhado (que será mantido como
    uma coluna de struct).  Caso deseje normalizar o coordenador em
    outra dimensão, essa transformação deve ocorrer na camada Silver.
    """
    records = get_api_data("frentes")
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))
    save_bronze(df, "frentes", spark)
    return df


def ingest_frentes_membros(spark: SparkSession) -> DataFrame:
    """Ingesta a lista de membros de cada frente.

    Para cada frente recuperada em `ingest_frentes`, consulta o endpoint
    `/frentes/{id}/membros` e agrega todos os integrantes.  A coluna
    `idFrente` é adicionada para manter o relacionamento de chave
    estrangeira.
    """
    frentes_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "frentes"))
    frentes_ids = [row.id for row in frentes_df.select("id").distinct().collect()]
    all_members: List[Dict] = []
    for fid in frentes_ids:
        members = get_api_data(f"frentes/{fid}/membros")
        for m in members:
            m["idFrente"] = fid
        all_members.extend(members)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in all_members]))
    save_bronze(df, "frentes_membros", spark)
    return df


def ingest_deputados(spark: SparkSession) -> DataFrame:
    """Ingesta a lista de deputados em exercício.

    Este endpoint retorna apenas os deputados na legislatura atual; para
    recuperar deputados de legislaturas anteriores, utilize o
    parâmetro `idLegislatura`.  O DataFrame resultante inclui dados
    pessoais, partido, UF, legislatura, entre outros.
    """
    records = get_api_data("deputados")
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))
    save_bronze(df, "deputados", spark)
    return df


def ingest_eventos(spark: SparkSession, ano: Optional[int] = None) -> DataFrame:
    """Ingesta eventos legislativos.

    A API de eventos permite filtrar por data inicial e final ou
    simplesmente retornar os eventos futuros.  Para simplificar,
    disponibilizamos um parâmetro `ano` que, quando informado,
    limita a busca ao ano calendárico (por exemplo, 2025).  Caso
    `ano` seja None, todos os eventos serão carregados.
    """
    params = {}
    if ano:
        # define intervalo de datas para o ano inteiro
        params["dataInicio"] = f"{ano}-01-01"
        params["dataFim"] = f"{ano}-12-31"
    records = get_api_data("eventos", params=params)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))
    save_bronze(df, f"eventos{'_'+str(ano) if ano else ''}", spark)
    return df


def ingest_votacoes(spark: SparkSession, data_inicio: Optional[str] = None, data_fim: Optional[str] = None) -> DataFrame:
    """Ingesta votações ocorridas no período informado.

    O período pode ser especificado através de strings no formato
    AAAA-MM-DD.  Se nenhum período for informado, a função faz
    download de todas as votações disponíveis na API, o que pode
    resultar em grande volume de dados.
    """
    params = {}
    if data_inicio:
        params["dataInicio"] = data_inicio
    if data_fim:
        params["dataFim"] = data_fim
    records = get_api_data("votacoes", params=params)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))
    save_bronze(df, "votacoes", spark)
    return df


def ingest_votos_deputados(spark: SparkSession, votacao_ids: Optional[Iterable[int]] = None) -> DataFrame:
    """Ingesta os votos por deputado para cada votação especificada.

    Se `votacao_ids` for None, a função consulta o DataFrame bronze
    de votações para descobrir os IDs de votação.  Para cada ID, o
    endpoint `/votacoes/{id}/votos` é chamado e os votos são
    consolidados em um DataFrame.  A coluna `idVotacao` é adicionada
    para manter o relacionamento.
    """
    if votacao_ids is None:
        vot_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "votacoes"))
        votacao_ids = [row.id for row in vot_df.select("id").distinct().collect()]
    all_votes: List[Dict] = []
    for vid in votacao_ids:
        votes = get_api_data(f"votacoes/{vid}/votos")
        for v in votes:
            v["idVotacao"] = vid
        all_votes.extend(votes)
    if not all_votes:
        return spark.createDataFrame([], schema=None)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in all_votes]))
    save_bronze(df, "votos_deputados", spark)
    return df


def ingest_despesas_deputados(spark: SparkSession, deputado_ids: Optional[Iterable[int]] = None) -> DataFrame:
    """Ingesta despesas por deputado de forma incremental.

    Este endpoint requer paginação explícita através dos parâmetros
    `pagina` e `itens`.  A API retorna até 100 itens por página.  A
    função itera em todas as páginas para cada deputado e adiciona
    colunas de controle (`idDeputado`, `anoMes`) que auxiliam na
    identificação incremental.  O ano e mês são extraídos da data do
    documento.
    """
    if deputado_ids is None:
        dep_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "deputados"))
        deputado_ids = [row.id for row in dep_df.select("id").distinct().collect()]
    all_expenses: List[Dict] = []
    for dep_id in deputado_ids:
        page = 1
        while True:
            params = {"pagina": page, "itens": 100}
            expenses = get_api_data(f"deputados/{dep_id}/despesas", params=params)
            if not expenses:
                break
            for e in expenses:
                e["idDeputado"] = dep_id
            all_expenses.extend(expenses)
            page += 1
    if not all_expenses:
        return spark.createDataFrame([], schema=None)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in all_expenses]))
    save_bronze(df, "despesas_deputados", spark)
    return df


def ingest_cpis(spark: SparkSession) -> DataFrame:
    """Ingesta dados de Comissões Parlamentares de Inquérito (CPIs).

    A API de CPIs está disponível através do endpoint `/eventos` com
    filtros de tipo ou usando o endpoint `/proposicoes` para
    recuperações de proposições de CPIs.  Este exemplo utiliza
    `/proposicoes` com filtro de tipo igual a 'CPIPC'.  Ajustes podem
    ser necessários dependendo da evolução da API.
    """
    params = {"siglaTipo": "CPIPC"}
    records = get_api_data("proposicoes", params=params)
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))
    save_bronze(df, "cpis", spark)
    return df


def ingest_all(spark: SparkSession) -> None:
    """Executa todas as ingestões disponíveis.

    A ordem de ingestão é importante quando outras ingestões dependem
    dos dados previamente gravados.  Por exemplo, `frentes_membros`
    depende dos IDs de frentes; `votos_deputados` depende de
    `votacoes`.
    """
    ingest_frentes(spark)
    ingest_frentes_membros(spark)
    ingest_deputados(spark)
    ingest_eventos(spark)
    ingest_votacoes(spark)
    ingest_votos_deputados(spark)
    ingest_despesas_deputados(spark)
    ingest_cpis(spark)
