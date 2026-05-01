# Instruções de Execução – Projeto Câmara Brasil

Este guia passo a passo descreve como executar os pipelines do
projeto **camara_brasil** no Databricks Community Edition (Free
Edition) até a geração das tabelas gold e dashboards.  As
instruções assumem que você já possui uma conta no Databricks.

## 1. Preparação do Ambiente

1. Faça login em [community.cloud.databricks.com](https://community.cloud.databricks.com) e crie um **Workspace** caso ainda não exista.
2. Acesse o menu **Compute** e clique em **Create Cluster**.  Escolha um nome (por exemplo, `camara_brasil_cluster`), mantenha a versão do Runtime (recomenda‑se 13.x ou superior com Spark 3.5) e selecione o tamanho mais simples disponível.  Clique em **Create Cluster**.
3. No canto inferior esquerdo, clique no ícone de **Data** e
   selecione **Create Table** → **Upload File**.  Faça upload do
   diretório `camara_brasil` inteiro (ou compacte e faça upload de um
   ZIP).  O Databricks criará uma pasta `/FileStore/tables/camara_brasil`.
4. (Opcional) Crie **mount points** se você possuir acesso a uma
   conta de armazenamento externa (Amazon S3, Azure Data Lake).  Para
   este projeto foram utilizadas as pastas **/mnt/bronze**,
   **/mnt/silver** e **/mnt/gold**.  Na Community Edition você pode
   simplesmente utilizar `/FileStore/bronze`, `/FileStore/silver` e
   `/FileStore/gold` substituindo as constantes `BRONZE_PATH`,
   `SILVER_PATH` e `GOLD_PATH` nos scripts.

## 2. Ingestão da Camada Bronze

1. No menu **Workspace**, clique com o botão direito sobre a pasta
   importada `camara_brasil/scripts` e selecione **Import**.
2. Importe o arquivo `bronze_ingestao.py` como notebook.  Uma vez
   importado, abra o notebook e anexe‑o ao seu cluster.
3. Ajuste as variáveis `BRONZE_PATH` (se necessário) na primeira
   célula.  Por exemplo:

   ```python
   BRONZE_PATH = "dbfs:/FileStore/bronze/camara_brasil"
   ```

4. Execute a célula `ingest_all(spark)` no final do notebook.  O
   processo chamará todos os endpoints da API de forma paginada e
   gravará os arquivos Delta na pasta bronze.  Dependendo do número
   de deputados e despesas, essa etapa pode levar alguns minutos.

5. Verifique a criação dos diretórios no **Data Explorer** ou com
   comandos `dbutils.fs.ls("/FileStore/bronze/camara_brasil")` para
   confirmar que as tabelas foram gravadas.

## 3. Transformação da Camada Silver

1. Importe o arquivo `silver_transformacao.py` como notebook e
   anexe‑o ao mesmo cluster.
2. Atualize a variável `BRONZE_PATH` para apontar para a pasta de
   bronze e defina `SILVER_PATH`, por exemplo:

   ```python
   BRONZE_PATH = "dbfs:/FileStore/bronze/camara_brasil"
   SILVER_PATH = "dbfs:/FileStore/silver/camara_brasil"
   ```

3. Execute a célula `run_all_transformations(spark)` para criar
   todas as dimensões e fatos.  As tabelas serão gravadas em
   `/FileStore/silver/camara_brasil`.

4. (Opcional) Registre as tabelas como **tables** no metastore com
   comandos `spark.sql(f"CREATE TABLE IF NOT EXISTS silver.{table} USING DELTA LOCATION '{path}'")` para facilitar consultas SQL.

## 4. Geração da Camada Gold

1. Importe o arquivo `gold_analise.py` como notebook.
2. Defina `SILVER_PATH` e `GOLD_PATH` de acordo com os caminhos
   configurados (por exemplo, `/FileStore/silver/camara_brasil` e
   `/FileStore/gold/camara_brasil`).
3. Execute a célula `run_all_analytics(spark)`.  Serão criadas
   diversas tabelas Delta na pasta gold contendo os indicadores
   solicitados: índices de diversidade, correlação de votações,
   anomalias de despesas, ranking de fornecedores, densidade de eventos
   e scores de engajamento.
4. Registre as tabelas gold no metastore conforme o passo 3.4 para
   permitir consultas via Databricks SQL.

## 5. Criação de Dashboards

1. Acesse a aba **SQL** e clique em **Create Query**.  Selecione o
   seu cluster ou compute pool como warehouse e escolha o banco de
   dados correspondente.
2. Escreva consultas sobre as tabelas gold, por exemplo:

   ```sql
   SELECT fr.id_frente, d.indice_diversidade
   FROM gold.indice_diversidade_frentes d
   JOIN silver.dim_frente fr ON fr.id_frente = d.id_frente
   ORDER BY indice_diversidade DESC
   LIMIT 20;
   ```

3. Clique em **Visualization** e escolha o tipo de gráfico desejado (barra, pizza, heatmap, etc.).  Ajuste e salve.
4. Para compor um dashboard, clique em **Dashboards** → **Create Dashboard**, adicione widgets e arraste cada visualização salva.  Configure filtros globais (por exemplo, ano ou UF) para dinamizar a análise.

## 6. Automação e Agendamentos

1. No menu **Workflows**, crie um **Job** e adicione tarefas para
   cada notebook (ingestão bronze, transformação silver e análises
   gold).  Defina dependências para garantir a ordem correta.
2. Configure um cron (por exemplo, `0 8 * * *` para executar
   diariamente às 08h) para manter os dados atualizados.
3. Ative notificações em caso de falha e revise os logs disponíveis
   em **Runs** para investigar erros.

## 7. Considerações

- A API da Câmara possui limites de requisições; caso ocorra `HTTP 429` aguarde alguns segundos e reexecute a carga.
- Alguns endpoints podem retornar campos adicionais ou mudarem com o
  tempo.  Os scripts foram modularizados para facilitar ajustes sem
  impactar o restante do pipeline.
- A Community Edition possui limitações de armazenamento e
  processamento; caso sua carga ultrapasse esses limites, considere
  subir o projeto para uma instância Databricks paga ou executar as
  etapas de ingestão fora do ambiente (Spark local) e importar apenas
  os Delta resultantes.

Seguindo estes passos você terá uma plataforma de análise completa
sobre dados legislativos, desde a ingestão até a visualização.
