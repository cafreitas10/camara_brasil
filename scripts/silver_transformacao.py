"""
Transformações da camada Silver para o projeto camara_brasil.

Este módulo define funções que consomem as tabelas bronze geradas
pelas ingestões e produzem tabelas estruturadas (dimensões e fatos)
na camada Silver.  Aqui são aplicadas limpezas, normalizações de
campos, separação de entidades e, quando apropriado, técnicas de
captura de mudanças (SCD Type 2).  A camada Silver prepara os dados
para análises mais profundas e para servir de base às agregações da
camada Gold.

Cada função recebe uma SparkSession e grava as tabelas resultantes
em caminhos específicos da Silver.  Para facilitar a leitura,
dimensões e fatos seguem o padrão `dim_*` e `fato_*`.

Nota: Para simplificar, os exemplos de SCD Type 2 foram reduzidos
apenas à dimensão de deputados, pois muitos atributos podem variar
entre legislaturas (partido, UF, gabinete, etc.).  Caso outras
dimensões exijam versionamento, utilize o mesmo padrão.
"""

import os
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Caminho padrão para leitura dos dados bronze e gravação silver
BRONZE_PATH = "dbfs:/mnt/bronze/camara_brasil"
SILVER_PATH = "dbfs:/mnt/silver/camara_brasil"


def write_silver(df: DataFrame, table_name: str) -> None:
    """Grava o DataFrame na camada Silver em formato Delta.

    Args:
        df: DataFrame pronto para ser gravado.
        table_name: Nome da tabela (subdiretório em SILVER_PATH).
    """
    path = os.path.join(SILVER_PATH, table_name)
    (df
     .write
     .format("delta")
     .mode("overwrite")
     .save(path))


def create_dim_deputado(spark: SparkSession) -> DataFrame:
    """Cria a dimensão de deputados com versão histórica (SCD Type 2).

    A tabela bronze `deputados` contém apenas os deputados em exercício,
    portanto o versionamento será aplicado com base no campo
    `dataInicio` e `dataFim` que virão de endpoints históricos (não
    disponíveis diretamente neste projeto).  Neste exemplo, assume-se
    que cada carga substitui por completo os deputados em exercício e
    que novas versões são atribuídas a partir da data de processamento.
    """
    df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "deputados"))
    # renomeia colunas para evitar conflitos e seleciona campos de interesse
    df_sel = (df
              .withColumnRenamed("id", "id_deputado")
              .select(
                  "id_deputado",
                  F.col("nome").alias("nome_deputado"),
                  F.col("siglaPartido").alias("sigla_partido"),
                  F.col("uriPartido").alias("uri_partido"),
                  F.col("siglaUf").alias("sigla_uf"),
                  F.col("idLegislatura").alias("id_legislatura"),
                  F.col("dataNascimento").alias("data_nascimento"),
                  F.current_timestamp().alias("valid_from")
              )
              )
    # define campos SCD Type 2
    df_sc2 = (df_sel
              .withColumn("valid_to", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True))
              )
    write_silver(df_sc2, "dim_deputado")
    return df_sc2


def create_dim_partido(spark: SparkSession) -> DataFrame:
    """Cria a dimensão de partidos a partir da dimensão de deputados.

    O endpoint de partidos poderia ser utilizado para enriquecer esta
    dimensão, mas neste projeto derivamos os partidos a partir dos
    registros de deputados.  A dimensão contém apenas campos
    `sigla_partido` e a data de processamento.
    """
    dep_df = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_deputado"))
    part_df = (dep_df
               .select("sigla_partido")
               .distinct()
               .withColumnRenamed("sigla_partido", "sigla_partido")
               .withColumn("valid_from", F.current_timestamp())
               .withColumn("valid_to", F.lit(None).cast("timestamp"))
               .withColumn("is_current", F.lit(True))
               )
    write_silver(part_df, "dim_partido")
    return part_df


def create_dim_frente(spark: SparkSession) -> DataFrame:
    """Cria a dimensão de frentes parlamentares.

    Os dados de frentes vêm da tabela bronze `frentes`.  A
    normalização consiste em renomear campos, extrair a data de
    criação e manter o coordenador como struct.  Caso deseje
    normalizar o coordenador em outra dimensão, isso deve ser feito
    posteriormente.
    """
    fr_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "frentes"))
    dim_frente = (fr_df
                  .withColumnRenamed("id", "id_frente")
                  .withColumnRenamed("titulo", "titulo_frente")
                  .withColumn("data_criacao", F.to_date("dataCriacao"))
                  .withColumn("id_legislatura", F.col("idLegislatura"))
                  .withColumn("valid_from", F.current_timestamp())
                  .withColumn("valid_to", F.lit(None).cast("timestamp"))
                  .withColumn("is_current", F.lit(True))
                  .select("id_frente", "titulo_frente", "data_criacao", "id_legislatura", "coordenador", "valid_from", "valid_to", "is_current")
                  )
    write_silver(dim_frente, "dim_frente")
    return dim_frente


def create_fact_frente_membro(spark: SparkSession) -> DataFrame:
    """Cria a tabela fato para membros de frentes.

    Esta tabela relaciona cada deputado a uma frente parlamentar.  A
    chave composta é formada pelos IDs de frente e deputado.  Colunas
    adicionais, como data de adesão, podem ser incluídas se
    disponibilizadas pela API.
    """
    membros_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "frentes_membros"))
    fact_df = (membros_df
               .withColumnRenamed("id", "id_deputado")
               .withColumnRenamed("idFrente", "id_frente")
               .withColumn("data_referencia", F.current_date())
               .select("id_frente", "id_deputado", "data_referencia")
               )
    write_silver(fact_df, "fato_frente_membro")
    return fact_df


def create_dim_evento(spark: SparkSession) -> DataFrame:
    """Cria dimensões relacionadas a eventos (órgão, tipo e data).

    O DataFrame de eventos é preparado na camada bronze.  Esta
    função extrai dimensões normalizadas para órgão (`dim_orgao`),
    tipo de evento (`dim_tipo_evento`) e data (`dim_data`).  Cada
    dimensão recebe um identificador surrogate gerado por hash.
    """
    eventos_path = os.path.join(BRONZE_PATH, "eventos")
    ev_df = spark.read.format("delta").load(eventos_path)

    # Dimensão data
    dim_data = (ev_df
                .withColumn("data", F.to_date("dataHoraInicio"))
                .select("data")
                .distinct()
                .withColumn("id_data", F.monotonically_increasing_id())
                )
    write_silver(dim_data, "dim_data")

    # Dimensão órgão (ex.: comissão, plenário)
    # Cada evento pode ter vários órgãos, mas na API v2 a lista vem em
    # `orgaos` ou `orgao` aninhado.  Normalizamos listando cada
    # órgão separadamente.
    if "orgaos" in ev_df.columns:
        org_df = ev_df.select(F.explode("orgaos").alias("org"))
    elif "orgao" in ev_df.columns:
        org_df = ev_df.select(F.col("orgao").alias("org"))
    else:
        org_df = spark.createDataFrame([], schema="id string, sigla string, nome string")
    dim_orgao = (org_df
                 .select(F.col("org.id").alias("id_orgao"),
                         F.col("org.sigla").alias("sigla_orgao"),
                         F.col("org.nome").alias("nome_orgao"))
                 .distinct()
                 .withColumn("id_orgao_dim", F.monotonically_increasing_id())
                 )
    write_silver(dim_orgao, "dim_orgao")

    # Dimensão tipo de evento
    dim_tipo = (ev_df
                .select(F.col("descricaoTipo"))
                .distinct()
                .withColumnRenamed("descricaoTipo", "descricao_tipo")
                .withColumn("id_tipo_evento", F.monotonically_increasing_id())
                )
    write_silver(dim_tipo, "dim_tipo_evento")

    return dim_data


def create_fact_evento(spark: SparkSession) -> DataFrame:
    """Cria a tabela fato de eventos.

    A tabela fato relaciona cada evento às dimensões geradas e
    traz medidas como duração (em horas), número de participantes
    conhecidos e informação de quórum.  Para simplificar, a medida
    quantidade de participantes é derivada do tamanho da lista de
    presenças quando disponível.
    """
    ev_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "eventos"))
    dim_data = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_data"))
    dim_tipo = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_tipo_evento"))
    # Unir com dim_data por data
    fato = (ev_df
            .withColumn("data", F.to_date("dataHoraInicio"))
            .join(dim_data, on="data", how="left")
            .join(dim_tipo, ev_df.descricaoTipo == dim_tipo.descricao_tipo, how="left")
            .withColumn("duracao_horas", (F.unix_timestamp("dataHoraFim") - F.unix_timestamp("dataHoraInicio")) / 3600.0)
            .withColumn("qtde_presencas", F.size(F.col("presenca")) if "presenca" in ev_df.columns else F.lit(None))
            .select(
                F.col("id").alias("id_evento"),
                "id_data", "id_tipo_evento",
                "duracao_horas", "qtde_presencas"
            )
            )
    write_silver(fato, "fato_evento")
    return fato


def create_fact_voto(spark: SparkSession) -> DataFrame:
    """Cria a tabela fato de votos por deputado.

    Une os votos dos deputados às dimensões de deputados e de
    votações, gerando medidas de alinhamento.  O campo `voto` indica
    o posicionamento (Sim, Não, etc.).  Para análises de correlação,
    valores textuais serão convertidos para indicadores numéricos na
    camada Gold.
    """
    votos_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "votos_deputados"))
    dim_dep = spark.read.format("delta").load(os.path.join(SILVER_PATH, "dim_deputado"))
    fato = (votos_df
            .withColumnRenamed("id", "id_deputado")
            .join(dim_dep, on="id_deputado", how="left")
            .select("id_deputado", "idVotacao", F.col("voto").alias("voto_texto"))
            )
    write_silver(fato, "fato_voto")
    return fato


def create_fact_despesa(spark: SparkSession) -> DataFrame:
    """Cria a tabela fato de despesas por deputado.

    A tabela bronze `despesas_deputados` contém diversas informações
    textuais.  Aqui, normalizamos a data de emissão da despesa,
    extraímos o mês e criamos dimensões de fornecedor e categoria.
    Medidas monetárias são convertidas para tipo decimal.  Assume-se
    que a coluna `valorLiquido` esteja disponível; ajuste se o nome
    diferir.
    """
    des_df = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "despesas_deputados"))
    # Dimensão fornecedor
    dim_fornecedor = (des_df
                      .select(F.col("fornecedor"))
                      .distinct()
                      .withColumnRenamed("fornecedor", "nome_fornecedor")
                      .withColumn("id_fornecedor", F.monotonically_increasing_id())
                      )
    write_silver(dim_fornecedor, "dim_fornecedor")
    # Dimensão categoria
    dim_categoria = (des_df
                     .select(F.col("tipoDespesa").alias("categoria"))
                     .distinct()
                     .withColumn("id_categoria", F.monotonically_increasing_id())
                     )
    write_silver(dim_categoria, "dim_categoria")
    # Fato despesa
    fato = (des_df
            .withColumnRenamed("idDeputado", "id_deputado")
            .withColumn("data_documento", F.to_date("dataDocumento"))
            .withColumn("ano_mes", F.date_format("data_documento", "yyyy-MM"))
            .join(dim_fornecedor, des_df.fornecedor == dim_fornecedor.nome_fornecedor, how="left")
            .join(dim_categoria, des_df.tipoDespesa == dim_categoria.categoria, how="left")
            .select(
                "id_deputado", "id_fornecedor", "id_categoria",
                "ano_mes", F.col("valorLiquido").cast("double").alias("valor_liquido")
            )
            )
    write_silver(fato, "fato_despesa")
    return fato


def run_all_transformations(spark: SparkSession) -> None:
    """Executa todas as transformações de criação de tabelas Silver.

    A ordem é importante pois algumas dimensões dependem de outras.
    """
    dim_dep = create_dim_deputado(spark)
    create_dim_partido(spark)
    create_dim_frente(spark)
    create_fact_frente_membro(spark)
    create_dim_evento(spark)
    create_fact_evento(spark)
    create_fact_voto(spark)
    create_fact_despesa(spark)
    # CPIs e outras dimensões/fatos podem ser adicionadas aqui
