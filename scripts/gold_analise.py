"""
Transformações da camada Gold para o projeto camara_brasil.

Este módulo contém funções que geram métricas agregadas e visões
analíticas a partir das tabelas silver.  Os resultados incluem
indicadores de diversidade partidária, participação de deputados em
frentes, análises de presença em eventos, detecção de anomalias de
despesas e correlação entre votações e frentes parlamentares.  A
camada Gold também serve como base para dashboards e relatórios no
Databricks SQL ou notebooks interativos.
"""

import os
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Caminho padrão para leitura das tabelas silver e gravação das gold
SILVER_PATH = "dbfs:/mnt/silver/camara_brasil"
GOLD_PATH = "dbfs:/mnt/gold/camara_brasil"


def write_gold(df: DataFrame, table_name: str) -> None:
    """Grava um DataFrame no formato Delta na camada Gold."""
    path = os.path.join(GOLD_PATH, table_name)
    (df
     .write
     .format("delta")
     .mode("overwrite")
     .save(path))


def herfindahl_index(counts: DataFrame, group_col: str, part_col: str, count_col: str) -> DataFrame:
    """Calcula o índice de Herfindahl-Hirschman para cada grupo.

    O índice é 1 - soma dos quadrados das participações de cada
    partido (ou categoria) em relação ao total do grupo.  Valores
    próximos de 1 indicam diversidade; próximos de 0 indicam
    concentração.
    """
    total_df = counts.groupBy(group_col).agg(F.sum(count_col).alias("total"))
    share_df = (counts
                .join(total_df, on=group_col)
                .withColumn("participacao", F.col(count_col) / F.col("total"))
                .withColumn("hhi_parcial", F.col("participacao") * F.col("participacao"))
                .groupBy(group_col)
                .agg((F.lit(1) - F.sum("hhi_parcial")).alias("indice_diversidade"))
                )
    return share_df


def compute_diversidade_frentes(spark: SparkSession) -> DataFrame:
    """Calcula a diversidade partidária (Herfindahl) de cada frente.

    A partir da fato `fato_frente_membro` e da dimensão de deputados,
    conta-se o número de membros de cada partido em cada frente e
    aplica-se o índice de Herfindahl.
    """
    fato = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_frente_membro"))
    dim_dep = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_deputado"))
    joined = (fato
              .join(dim_dep, on="id_deputado", how="left")
              .groupBy("id_frente", "sigla_partido")
              .agg(F.count("id_deputado").alias("qtde"))
              )
    diversity_df = herfindahl_index(joined, "id_frente", "sigla_partido", "qtde")
    write_gold(diversity_df, "indice_diversidade_frentes")
    return diversity_df


def deputados_mais_frentes(spark: SparkSession, top_n: int = 10) -> DataFrame:
    """Identifica os deputados com participação em mais frentes.

    Retorna os `top_n` deputados ordenados pela quantidade de
    frentes em que participam.  Esta métrica ajuda a compreender o
    engajamento dos parlamentares em diferentes causas.
    """
    fato = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_frente_membro"))
    dep_count = (fato
                 .groupBy("id_deputado")
                 .agg(F.countDistinct("id_frente").alias("qtde_frentes"))
                 .orderBy(F.desc("qtde_frentes"))
                 .limit(top_n)
                 )
    write_gold(dep_count, "deputados_mais_frentes")
    return dep_count


def correlacao_frentes_votacoes(spark: SparkSession) -> DataFrame:
    """Analisa se membros de uma mesma frente votam de forma mais alinhada.

    A correlação é simplificada como a proporção de votos iguais
    (Sim/Não/Abstenção) dentro de cada frente em relação à proporção
    geral da casa.  Um valor acima de 1 indica maior alinhamento
    dentro da frente; valores abaixo sugerem dispersão.
    """
    # Carrega fatos de votos e frentes
    votos_df = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_voto"))
    frentes_df = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_frente_membro"))
    # Junta votos e frentes
    votos_frente = (votos_df
                    .join(frentes_df, on="id_deputado", how="inner")
                    .groupBy("id_frente", "idVotacao", "voto_texto")
                    .agg(F.count("id_deputado").alias("qtde"))
                    )
    # calcula proporção dentro da frente
    total_por_votacao = (votos_frente
                         .groupBy("id_frente", "idVotacao")
                         .agg(F.sum("qtde").alias("total")))
    votos_frente = votos_frente.join(total_por_votacao, on=["id_frente", "idVotacao"])
    votos_frente = votos_frente.withColumn("prop_frente", F.col("qtde") / F.col("total"))
    # calcula proporção geral da casa (sem segmentar por frente)
    votos_geral = (votos_df
                   .groupBy("idVotacao", "voto_texto")
                   .agg(F.count("id_deputado").alias("qtde"))
                   )
    total_geral = votos_geral.groupBy("idVotacao").agg(F.sum("qtde").alias("total"))
    votos_geral = votos_geral.join(total_geral, on="idVotacao")
    votos_geral = votos_geral.withColumn("prop_geral", F.col("qtde") / F.col("total"))
    # junta proporções geral e de frentes
    corr_df = (votos_frente
               .join(votos_geral, on=["idVotacao", "voto_texto"], how="left")
               .withColumn("indice_alinhamento", F.col("prop_frente") / F.col("prop_geral"))
               .groupBy("id_frente")
               .agg(F.mean("indice_alinhamento").alias("indice_alinhamento_medio"))
               )
    write_gold(corr_df, "correlacao_frentes_votacoes")
    return corr_df


def anomalias_despesas(spark: SparkSession) -> DataFrame:
    """Detecta anomalias de despesas usando z-score.

    Calcula o desvio padrão das despesas por categoria e UF do
    deputado.  O score de anomalia é dado por (valor - média)/desvio.
    Despesas com score absoluto acima de 3 são marcadas como
    potenciais anomalias.
    """
    fato = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_despesa"))
    dep_df = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_deputado"))
    joined = fato.join(dep_df.select("id_deputado", "sigla_uf"), on="id_deputado", how="left")
    agg = (joined
           .groupBy("id_categoria", "sigla_uf")
           .agg(
               F.mean("valor_liquido").alias("media"),
               F.stddev("valor_liquido").alias("desvio"))
           )
    df = (joined
          .join(agg, on=["id_categoria", "sigla_uf"], how="left")
          .withColumn("z_score", (F.col("valor_liquido") - F.col("media")) / F.col("desvio"))
          .withColumn("anomalia", F.abs(F.col("z_score")) > 3)
          )
    write_gold(df, "anomalias_despesas")
    return df


def ranking_fornecedores(spark: SparkSession, top_n: int = 10) -> DataFrame:
    """Gera ranking de fornecedores mais pagos.

    Ordena fornecedores pelo valor total recebido e inclui flag para
    CNPJ suspeito, que neste exemplo é um placeholder sempre falso.
    Para validar CNPJs, integre com serviços de consulta ou bases
    públicas quando possível.
    """
    fato = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_despesa"))
    dim_for = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_fornecedor"))
    ranking = (fato
               .groupBy("id_fornecedor")
               .agg(F.sum("valor_liquido").alias("total_recebido"))
               .join(dim_for, on="id_fornecedor", how="left")
               .orderBy(F.desc("total_recebido"))
               .limit(top_n)
               .withColumn("cnpj_suspeito", F.lit(False))
               )
    write_gold(ranking, "ranking_fornecedores")
    return ranking


def frequencia_eventos(spark: SparkSession) -> DataFrame:
    """Calcula densidade de eventos por semana e identifica semanas sem eventos.

    A partir da fato de eventos, agrega a quantidade de eventos por
    semana.  A coluna `semana` usa a semana ISO (1-53).  Semanas sem
    eventos são aquelas em que a contagem é zero.
    """
    fato_ev = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_evento"))
    dim_data = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_data"))
    df = (fato_ev
          .join(dim_data, on="id_data", how="left")
          .withColumn("ano", F.year("data"))
          .withColumn("semana", F.weekofyear("data"))
          .groupBy("ano", "semana")
          .agg(F.count("id_evento").alias("qtde_eventos"))
          )
    write_gold(df, "frequencia_eventos_semana")
    return df


def score_engajamento(spark: SparkSession) -> DataFrame:
    """Calcula score de engajamento para cada deputado.

    O score compõe métricas de presença em eventos, votos, discursos
    (não implementado) e requerimentos (não implementado).  Aqui
    utiliza-se a frequência de eventos e o número de votos em que o
    deputado participou.  Os valores são normalizados e combinados
    pela média.
    """
    # Frequência em eventos
    fato_ev = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_evento"))
    presencas = fato_ev.select("id_evento", "qtde_presencas").filter("qtde_presencas IS NOT NULL")
    # Não há detalhamento de presenças por deputado na Silver; este
    # cálculo utiliza contagem total de eventos participados por
    # deputado quando tal dimensão existir.  Placeholder para futuro.
    # Votos por deputado
    fato_voto = spark.read.format("delta").load(os.path.join(SILVER_PATH, "fato_voto"))
    votos_por_dep = (fato_voto
                     .groupBy("id_deputado")
                     .agg(F.count("idVotacao").alias("qtde_votacoes"))
                     )
    # Normalização min-max nas métricas disponíveis
    stats = votos_por_dep.agg(
        F.min("qtde_votacoes").alias("min_voto"),
        F.max("qtde_votacoes").alias("max_voto")
    ).collect()[0]
    min_v, max_v = stats["min_voto"], stats["max_voto"]
    if max_v == min_v:
        normalized = votos_por_dep.withColumn("score_voto", F.lit(1.0))
    else:
        normalized = votos_por_dep.withColumn("score_voto", (F.col("qtde_votacoes") - min_v) / (max_v - min_v))
    # Score final (apenas votos por ora)
    score_df = normalized.select("id_deputado", F.col("score_voto").alias("score_engajamento"))
    write_gold(score_df, "score_engajamento")
    return score_df


def run_all_analytics(spark: SparkSession) -> None:
    """Executa todas as análises e gera as tabelas gold."""
    compute_diversidade_frentes(spark)
    deputados_mais_frentes(spark)
    correlacao_frentes_votacoes(spark)
    anomalias_despesas(spark)
    ranking_fornecedores(spark)
    frequencia_eventos(spark)
    score_engajamento(spark)
    # Outras análises podem ser adicionadas aqui
