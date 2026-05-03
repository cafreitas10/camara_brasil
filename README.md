# 🏛️ Pipeline Medallion - Câmara dos Deputados

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.x-green.svg)
![Databricks](https://img.shields.io/badge/Databricks-Serverless-red.svg)
![Status](https://img.shields.io/badge/status-active-success.svg)

Pipeline de dados end-to-end da Câmara dos Deputados do Brasil, implementando a arquitetura **Medallion** (Bronze/Silver/Gold) em Databricks com Delta Lake e Unity Catalog.

---

## 📖 Visão Geral

Este projeto implementa um pipeline completo de ingestão, transformação e análise de dados públicos da Câmara dos Deputados brasileira, utilizando a [API Dados Abertos](https://dadosabertos.camara.leg.br/).

### Características principais:
- 🔄 **Arquitetura Medallion**: Bronze (raw), Silver (curated), Gold (analytics)
- 📊 **827.643 linhas** processadas em **23 tabelas Delta**
- ⚡ **Ingestão paralela** com retry logic e tratamento de erros
- 🏗️ **Modelo dimensional** Star Schema na camada Silver
- 📈 **KPIs analíticos**: engajamento, diversidade, anomalias
- 📝 **Documentação automática** de tabelas em português
- 📊 **2 Dashboards interativos** com 8 visualizações

---

## 🏗️ Arquitetura

### Diagrama da Arquitetura Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                     API Dados Abertos Câmara                     │
│          https://dadosabertos.camara.leg.br/swagger/api.html    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                         🟤 BRONZE LAYER                          │
│                      (Raw Data - As Is)                          │
│                                                                  │
│  • bronze_camara_deputados              513 linhas              │
│  • bronze_camara_despesas_deputados     125.058 linhas          │
│  • bronze_camara_eventos_2024           2.479 linhas            │
│  • bronze_camara_frentes                1.440 linhas            │
│  • bronze_camara_frentes_membros        260.307 linhas          │
│  • bronze_camara_votacoes               1.854 linhas            │
│  • bronze_camara_votos_deputados        23.552 linhas           │
│                                                                  │
│  TOTAL: 7 tabelas | 415.203 linhas                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        ⚪ SILVER LAYER                           │
│                  (Curated Data - Star Schema)                   │
│                                                                  │
│  📐 DIMENSÕES:                                                   │
│    • silver_camara_dim_deputados        513 linhas              │
│    • silver_camara_dim_frentes          1.440 linhas            │
│    • silver_camara_dim_partidos         21 linhas               │
│                                                                  │
│  📊 FATOS:                                                       │
│    • silver_camara_fact_despesas        120.149 linhas          │
│    • silver_camara_fact_eventos         2.472 linhas            │
│    • silver_camara_fact_frentes_membros 260.304 linhas          │
│    • silver_camara_fact_votacoes        23.552 linhas           │
│                                                                  │
│  TOTAL: 7 tabelas | 408.451 linhas                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                         🟡 GOLD LAYER                            │
│                   (Analytics & Aggregations)                    │
│                                                                  │
│  • gold_camara_engajamento_deputados             513 linhas     │
│  • gold_camara_diversidade_partidaria_frentes    1.223 linhas   │
│  • gold_camara_anomalias_despesas                1.654 linhas   │
│  • gold_camara_despesas_por_tipo                 19 linhas      │
│  • gold_camara_despesas_por_deputado             487 linhas     │
│  • gold_camara_despesas_por_uf                   27 linhas      │
│  • gold_camara_ranking_fornecedores              50 linhas      │
│  • gold_camara_padroes_votacao                   5 linhas       │
│  • gold_camara_eventos_por_mes                   11 linhas      │
│                                                                  │
│  TOTAL: 9 tabelas | 3.989 linhas                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📚 Notebooks

O projeto é composto por 4 notebooks Jupyter executados sequencialmente:

| # | Notebook | Descrição | Tempo Exec. |
|---|----------|-----------|-------------|
| 1 | **01_Bronze_Ingestao** | Ingestão paralela de dados brutos da API com retry logic | ~10-15 min |
| 2 | **02_Silver_Transformacao** | Transformação em modelo dimensional Star Schema | ~5-8 min |
| 3 | **03_Gold_Analise** | Criação de métricas e KPIs analíticos | ~3-5 min |
| 4 | **00_Documentar_Tabelas** | Documentação automática de tabelas em PT-BR | ~2 min |

### Detalhamento

#### 1️⃣ Bronze - Ingestão
- **Ingestão paralela** com `ThreadPoolExecutor` (20 workers)
- **Retry logic** com backoff exponencial
- **Tratamento de erros** robusto (PySparkValueError, API timeouts)
- **Período**: Dezembro 2024 (configurável via parâmetros)

#### 2️⃣ Silver - Transformação
- **Modelo Star Schema**: 3 dimensões + 4 fatos
- **Limpeza de dados**: remoção de nulos, normalização
- **Enriquecimento**: junções e derivações
- **Validação**: testes de integridade referencial

#### 3️⃣ Gold - Análise
- **Score de engajamento** (0-100) para deputados
- **Índice de Simpson** para diversidade partidária
- **Detecção de anomalias** com z-score (|z| > 3)
- **Rankings**: top fornecedores, padrões de votação

#### 4️⃣ Documentação
- Comentários de tabela (`COMMENT ON TABLE`)
- Comentários de coluna (`ALTER TABLE ... ALTER COLUMN ... COMMENT`)
- **23 tabelas** documentadas com descrições em português

---

## 📊 Dashboards

O projeto inclui **2 dashboards interativos** desenvolvidos no Databricks AI/BI, organizados na pasta `/Users/cafreitas@gmail.com/dashboards/`, com um total de **8 widgets** e **28 visualizações** para análise exploratória dos dados da Câmara dos Deputados.

### Dashboards Disponíveis

| # | Dashboard | Widgets | Descrição | Tabelas Utilizadas |
|---|-----------|---------|-----------|-------------------|
| 1 | **Eventos Legislativos** | 4 | Análise temporal e tipológica de eventos parlamentares | `silver_camara_fact_eventos` |
| 2 | **Despesas por Categoria** | 4 | Análise financeira de despesas por tipo e categoria | `gold_camara_despesas_por_tipo` |

### 📈 Dashboard 1: Eventos Legislativos - Câmara dos Deputados

**Caminho:** `/Users/cafreitas@gmail.com/dashboards/Eventos Legislativos - Câmara dos Deputados.lvdash.json`

Dashboard focado em eventos legislativos ocorridos na Câmara dos Deputados, com análise temporal e distribuição por tipo de evento.

#### Widgets:
* **Counter** - Total de Eventos Registrados
  - Mostra o total de **2.472 eventos** legislativos capturados em 2024
  
* **Line Chart** - Eventos ao Longo do Tempo
  - Série temporal mostrando a evolução mensal dos eventos parlamentares
  - Visualiza padrões sazonais e picos de atividade legislativa
  
* **Bar Chart** - Top 10 Tipos de Eventos
  - Ranking horizontal dos tipos de eventos mais frequentes:
    - Reunião Deliberativa (851 eventos)
    - Audiência Pública (667 eventos)
    - Sessão Não Deliberativa Solene (167 eventos)
  
* **Table** - Eventos Recentes
  - Tabela interativa com os 20 eventos mais recentes
  - Colunas: Data/Hora, Tipo de Evento, Local
  - Ordenação por data descendente

#### Tabela fonte:
```sql
workspace.default.silver_camara_fact_eventos
```

---

### 💰 Dashboard 2: Despesas por Categoria - Câmara dos Deputados

**Caminho:** `/Users/cafreitas@gmail.com/dashboards/Despesas por Categoria - Câmara dos Deputados.lvdash.json`

Dashboard de análise financeira das despesas parlamentares, agrupadas por categoria de despesa.

#### Widgets:
* **Counter** - Total de Despesas
  - Exibe o montante total de **R$ 152,33 milhões** em despesas
  - Formatação em moeda brasileira (BRL) com abreviação compacta
  
* **Pie Chart** - Distribuição por Tipo de Despesa
  - Gráfico de pizza mostrando a proporção de gastos por categoria
  - Top 10 categorias com maior volume financeiro
  - Destaque para: Divulgação da Atividade Parlamentar, Passagem Aérea, Locação de Veículos
  
* **Bar Chart** - Top 10 Categorias por Valor
  - Gráfico de barras horizontal ordenado por valor total
  - Permite comparação visual rápida entre categorias
  - Ranking liderado por Divulgação da Atividade Parlamentar
  
* **Table** - Detalhes por Categoria
  - Tabela completa com 5 colunas:
    - Tipo de Despesa
    - Total Gasto (R$)
    - Média por Transação (R$)
    - Quantidade de Transações
    - Quantidade de Deputados
  - Ordenação por valor total descendente
  - Formatação monetária para valores

#### Tabela fonte:
```sql
workspace.default.gold_camara_despesas_por_tipo
```

---

### 🎨 Características dos Dashboards

#### Design e Layout:
- ✅ **Layout responsivo** em grade 2x2 para todos os dashboards
- ✅ **Tema escuro** profissional com high contrast
- ✅ **Títulos descritivos** em português brasileiro
- ✅ **Formatação monetária** automática (R$ com abreviação compacta)
- ✅ **Ordenação inteligente** (DESC por valores/datas)

#### Interatividade:
- ✅ **Tabelas ordenáveis** por qualquer coluna
- ✅ **Tooltip em gráficos** com valores detalhados
- ✅ **Drill-down** disponível em todos os widgets
- ✅ **Refresh automático** ao atualizar dados fonte

#### Performance:
- ✅ **Query otimizada** usando tabelas Gold agregadas
- ✅ **Cache de resultados** para respostas rápidas
- ✅ **Limite de 20 registros** em tabelas para performance

---

### 📂 Localização dos Dashboards

Os arquivos dos dashboards estão salvos em:

```
/Users/cafreitas@gmail.com/dashboards/
├── Eventos Legislativos - Câmara dos Deputados.lvdash.json
├── Despesas por Categoria - Câmara dos Deputados.lvdash.json
├── Anomalias em Despesas - Câmara dos Deputados.lvdash.json       (existente)
├── Ranking de Fornecedores - Câmara dos Deputados.lvdash.json     (existente)
└── Padrões de Votação - Câmara dos Deputados.lvdash.json          (existente)
```

**Total:** 5 dashboards publicados | 20+ widgets | 50+ visualizações

---

### 🔗 Como Acessar os Dashboards

#### Via Databricks Workspace:

1. **Acesse o workspace:**
   ```
   https://<seu-workspace>.cloud.databricks.com/
   ```

2. **Navegue até Dashboards:**
   - Menu lateral: **SQL > Dashboards**
   - Ou: Menu lateral: **Workspace > /Users/cafreitas@gmail.com/dashboards/**

3. **Abra o dashboard desejado:**
   - Clique no nome do dashboard
   - Os dados serão carregados automaticamente

#### Via Catalog Explorer:

1. Acesse: **Data > Catalog Explorer**
2. Navegue até: `workspace.default`
3. Visualize as tabelas fonte:
   - `silver_camara_fact_eventos`
   - `gold_camara_despesas_por_tipo`
4. Clique em **"Create Dashboard"** para criar novas visualizações

---

### 🔄 Atualização dos Dashboards

Os dashboards são automaticamente atualizados quando as tabelas fonte são reprocessadas:

```python
# Re-executar pipeline para atualizar dados
# 1. Execute o notebook Bronze (ingestão)
# 2. Execute o notebook Silver (transformação)
# 3. Execute o notebook Gold (análise)
# 4. Abra o dashboard - dados atualizados automaticamente
```

**Frequência recomendada:**
- **Dados históricos:** Mensal
- **Dados recentes:** Semanal
- **Eventos em andamento:** Diário

---

## 🛠️ Stack Tecnológico

| Componente | Tecnologia | Versão |
|------------|------------|--------|
| **Plataforma** | Databricks | Serverless Compute |
| **Processing** | PySpark | 3.x |
| **Storage** | Delta Lake | 2.x |
| **Catalog** | Unity Catalog | - |
| **Dashboards** | Databricks AI/BI | Lakeview |
| **Linguagem** | Python | 3.10+ |
| **API Client** | requests | 2.31+ |
| **Concorrência** | concurrent.futures | built-in |
| **Formato** | Parquet (Delta) | - |

---

## ✅ Pré-requisitos

### Infraestrutura
- ☑️ **Conta Databricks** (AWS/Azure/GCP)
- ☑️ **Unity Catalog** configurado
- ☑️ **Serverless Compute** habilitado (ou cluster DBR 13.3+)
- ☑️ **Permissões de escrita** no schema `workspace.default`

### Acesso
- ☑️ **API Dados Abertos da Câmara** (pública, sem autenticação)
- ☑️ **Conectividade internet** para acessar `dadosabertos.camara.leg.br`

### Opcional
- ☑️ **GitHub Personal Access Token** (para versionamento)
- ☑️ **Databricks Repos** configurado

---

## 🚀 Como Executar

### 📋 Passo 1: Configuração Inicial

#### 1.1 Clone o Repositório

```bash
# Via Git
git clone https://github.com/cafreitas10/camara_brasil.git
cd camara_brasil
```

#### 1.2 Configure Databricks Repos

1. Acesse Databricks workspace
2. Menu lateral: **Workspace > Repos**
3. Clique em **"Add Repo"**
4. Preencha:
   ```
   Git repository URL: https://github.com/cafreitas10/camara_brasil
   Git provider: GitHub
   Repository name: camara_brasil
   ```
5. Clique em **"Create Repo"**

---

### ⚙️ Passo 2: Configuração do Databricks

#### 2.1 Unity Catalog

Certifique-se de que o catálogo e schema existem:

```sql
-- Verificar catálogo
SHOW CATALOGS LIKE 'workspace';

-- Verificar schema
SHOW SCHEMAS IN workspace LIKE 'default';

-- Criar schema se não existir
CREATE SCHEMA IF NOT EXISTS workspace.default;
```

#### 2.2 Compute (Cluster)

**Opção A: Serverless (Recomendado)**
- Nenhuma configuração necessária
- Auto-selecionado ao executar células

**Opção B: Cluster Tradicional**
```
Databricks Runtime: 13.3 LTS ou superior
Node Type: Standard_DS3_v2 (ou equivalente)
Workers: 2-4
Autoscaling: Habilitado
```

---

### 🏃 Passo 3: Execução dos Notebooks

Execute os notebooks **na ordem abaixo**:

#### 📥 3.1 Bronze - Ingestão de Dados

```
Notebook: notebooks/01_Bronze_Ingestao.ipynb
```

**Parâmetros (já configurados):**
```python
ano = 2024
data_inicio = "2024-01-01"
data_fim = "2024-12-31"
```

**Passos:**
1. Abra o notebook `01_Bronze_Ingestao`
2. Anexe o cluster (ou deixe serverless)
3. Clique em **"Run All"** (ou Ctrl+Shift+Enter)
4. Aguarde conclusão: **~10-15 minutos**

**Resultado esperado:**
```
✅ 7 tabelas bronze_camara_* criadas
✅ 415.203 linhas ingeridas
✅ Período: dezembro/2024
```

**Validação:**
```sql
SHOW TABLES IN workspace.default LIKE 'bronze_camara_%';
SELECT COUNT(*) FROM workspace.default.bronze_camara_deputados; -- 513
```

---

#### 🔄 3.2 Silver - Transformação

```
Notebook: notebooks/02_Silver_Transformacao.ipynb
```

**Passos:**
1. Abra o notebook `02_Silver_Transformacao`
2. Anexe o cluster
3. Clique em **"Run All"**
4. Aguarde conclusão: **~5-8 minutos**

**Resultado esperado:**
```
✅ 7 tabelas silver_camara_* criadas
✅ 408.451 linhas transformadas
✅ Modelo: Star Schema (3 dim + 4 fatos)
```

**Validação:**
```sql
SHOW TABLES IN workspace.default LIKE 'silver_camara_%';
SELECT COUNT(*) FROM workspace.default.silver_camara_dim_deputados; -- 513
SELECT COUNT(*) FROM workspace.default.silver_camara_fact_despesas; -- 120.149
```

---

#### 📊 3.3 Gold - Análise

```
Notebook: notebooks/03_Gold_Analise.ipynb
```

**Passos:**
1. Abra o notebook `03_Gold_Analise`
2. Anexe o cluster
3. Clique em **"Run All"**
4. Aguarde conclusão: **~3-5 minutos**

**Resultado esperado:**
```
✅ 9 tabelas gold_camara_* criadas
✅ 3.989 linhas de métricas
✅ KPIs: Engajamento, diversidade, anomalias, rankings
```

**Validação:**
```sql
SHOW TABLES IN workspace.default LIKE 'gold_camara_%';
SELECT * FROM workspace.default.gold_camara_engajamento_deputados LIMIT 10;
SELECT * FROM workspace.default.gold_camara_anomalias_despesas LIMIT 10;
```

---

#### 📝 3.4 Documentação (Opcional)

```
Notebook: notebooks/00_Documentar_Tabelas.ipynb
```

**Passos:**
1. Abra o notebook `00_Documentar_Tabelas`
2. Anexe o cluster
3. Clique em **"Run All"**
4. Aguarde conclusão: **~2 minutos**

**Resultado esperado:**
```
✅ 23 tabelas documentadas
✅ Comentários de tabela aplicados
✅ Comentários de coluna aplicados (PT-BR)
```

**Validação:**
```sql
DESCRIBE EXTENDED workspace.default.silver_camara_dim_deputados;
```

---

#### 📊 3.5 Dashboards (Opcional)

Após executar os notebooks, acesse os dashboards criados:

1. **Navegue até Dashboards:**
   - Menu lateral: **SQL > Dashboards**
   - Ou: **Workspace > /Users/cafreitas@gmail.com/dashboards/**

2. **Abra os dashboards:**
   - **Eventos Legislativos - Câmara dos Deputados**
   - **Despesas por Categoria - Câmara dos Deputados**

3. **Explore as visualizações:**
   - Clique nos gráficos para drill-down
   - Ordene tabelas clicando nas colunas
   - Aplique filtros conforme necessário

---

### ✅ Passo 4: Validação Final

Execute as queries abaixo para confirmar que tudo está correto:

```sql
-- 1. Verificar todas as tabelas criadas
SHOW TABLES IN workspace.default LIKE '*camara*';
-- Resultado esperado: 23 tabelas

-- 2. Contar registros por camada
SELECT 
  'Bronze' AS camada,
  SUM(cnt) AS total_linhas
FROM (
  SELECT COUNT(*) AS cnt FROM workspace.default.bronze_camara_deputados
  UNION ALL SELECT COUNT(*) FROM workspace.default.bronze_camara_despesas_deputados
  UNION ALL SELECT COUNT(*) FROM workspace.default.bronze_camara_eventos_2024
  UNION ALL SELECT COUNT(*) FROM workspace.default.bronze_camara_frentes
  UNION ALL SELECT COUNT(*) FROM workspace.default.bronze_camara_frentes_membros
  UNION ALL SELECT COUNT(*) FROM workspace.default.bronze_camara_votacoes
  UNION ALL SELECT COUNT(*) FROM workspace.default.bronze_camara_votos_deputados
)
UNION ALL
SELECT 'Silver', SUM(cnt) FROM (
  SELECT COUNT(*) AS cnt FROM workspace.default.silver_camara_dim_deputados
  UNION ALL SELECT COUNT(*) FROM workspace.default.silver_camara_dim_frentes
  UNION ALL SELECT COUNT(*) FROM workspace.default.silver_camara_dim_partidos
  UNION ALL SELECT COUNT(*) FROM workspace.default.silver_camara_fact_despesas
  UNION ALL SELECT COUNT(*) FROM workspace.default.silver_camara_fact_eventos
  UNION ALL SELECT COUNT(*) FROM workspace.default.silver_camara_fact_frentes_membros
  UNION ALL SELECT COUNT(*) FROM workspace.default.silver_camara_fact_votacoes
)
UNION ALL
SELECT 'Gold', SUM(cnt) FROM (
  SELECT COUNT(*) AS cnt FROM workspace.default.gold_camara_anomalias_despesas
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_despesas_por_deputado
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_despesas_por_tipo
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_despesas_por_uf
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_diversidade_partidaria_frentes
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_engajamento_deputados
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_eventos_por_mes
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_padroes_votacao
  UNION ALL SELECT COUNT(*) FROM workspace.default.gold_camara_ranking_fornecedores
);

-- Resultado esperado:
-- Bronze: 415.203 linhas
-- Silver: 408.451 linhas
-- Gold: 3.989 linhas

-- 3. Testar queries analíticas
SELECT * FROM workspace.default.gold_camara_engajamento_deputados 
ORDER BY score_engajamento DESC 
LIMIT 10;

SELECT * FROM workspace.default.gold_camara_anomalias_despesas
WHERE abs(z_score) > 3
ORDER BY z_score DESC
LIMIT 10;
```

---

## 📁 Estrutura do Projeto

```
camara_brasil/
│
├── dashboards/                            # 📊 Dashboards Databricks AI/BI
│   ├── Eventos Legislativos.lvdash.json
│   └── Despesas por Categoria.lvdash.json
│
├── notebooks/
│   ├── 01_Bronze_Ingestao.ipynb          # Ingestão de dados brutos
│   ├── 02_Silver_Transformacao.ipynb     # Transformação Star Schema
│   ├── 03_Gold_Analise.ipynb             # Métricas e KPIs
│   └── 00_Documentar_Tabelas.ipynb       # Documentação automática
│
├── README.md                              # Este arquivo
└── .gitignore                             # Arquivos ignorados pelo Git
```

---

## 📊 Dados Gerados

### Resumo por Camada

| Camada | Tabelas | Linhas | % do Total |
|--------|---------|--------|------------|
| **Bronze** | 7 | 415.203 | 50,2% |
| **Silver** | 7 | 408.451 | 49,3% |
| **Gold** | 9 | 3.989 | 0,5% |
| **TOTAL** | **23** | **827.643** | **100%** |

### Top 5 Maiores Tabelas

| # | Tabela | Linhas | Camada |
|---|--------|--------|--------|
| 1 | silver_camara_fact_frentes_membros | 260.304 | Silver |
| 2 | bronze_camara_frentes_membros | 260.307 | Bronze |
| 3 | bronze_camara_despesas_deputados | 125.058 | Bronze |
| 4 | silver_camara_fact_despesas | 120.149 | Silver |
| 5 | bronze_camara_votos_deputados | 23.552 | Bronze |

---

## ✨ Features Principais

### Ingestão (Bronze)
- ✅ **Ingestão paralela** com `ThreadPoolExecutor` (20 workers simultâneos)
- ✅ **Retry logic** com backoff exponencial (máx 3 tentativas)
- ✅ **Tratamento robusto de erros**: PySparkValueError, API timeouts
- ✅ **Logging detalhado** para debug e monitoramento
- ✅ **Parâmetros configuráveis**: ano, período, max_workers

### Transformação (Silver)
- ✅ **Modelo dimensional Star Schema** (3 dimensões + 4 fatos)
- ✅ **Limpeza de dados**: remoção de nulos, normalização de strings
- ✅ **Enriquecimento**: junções e cálculos derivados
- ✅ **Validação de integridade**: foreign keys, duplicatas
- ✅ **Particionamento inteligente**: ano/mês para fatos grandes

### Análise (Gold)
- ✅ **Score de engajamento** (0-100) baseado em presenças e votações
- ✅ **Índice de Simpson** para medir diversidade partidária
- ✅ **Detecção de anomalias** usando z-score estatístico (|z| > 3)
- ✅ **Rankings dinâmicos**: top fornecedores, deputados, despesas
- ✅ **Agregações temporais**: eventos por mês, padrões de votação

### Documentação
- ✅ **Comentários automáticos** em PT-BR para tabelas e colunas
- ✅ **Visível no Catalog Explorer** e via `DESCRIBE EXTENDED`
- ✅ **23 tabelas documentadas** (7 Bronze + 7 Silver + 9 Gold)

### Dashboards
- ✅ **2 dashboards interativos** com 8 widgets totais
- ✅ **Visualizações profissionais**: counters, charts, tables
- ✅ **Layout responsivo** em grade 2x2
- ✅ **Formatação automática** de valores monetários e numéricos
- ✅ **Atualização automática** ao reprocessar dados

---

## 🐛 Troubleshooting

### Problema: Timeout na API

**Erro:**
```
ReadTimeout: HTTPSConnectionPool(host='dadosabertos.camara.leg.br', port=443)
```

**Solução:**
- Aumentar `timeout` nas chamadas HTTP (padrão: 30s)
- Reduzir `MAX_WORKERS` de 20 para 10
- Executar em horários de menor carga da API

---

### Problema: Memória Insuficiente

**Erro:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solução:**
- Usar cluster com mais memória (Standard_DS4_v2)
- Aumentar número de workers (4-8)
- Processar dados em batches menores (por mês)

---

### Problema: Tabelas Não Aparecem

**Erro:**
```
Table or view not found: workspace.default.bronze_camara_deputados
```

**Solução:**
```sql
-- Verificar schema
USE CATALOG workspace;
SHOW SCHEMAS;

-- Verificar tabelas
SHOW TABLES IN workspace.default;

-- Forçar refresh do cache (não necessário em serverless)
-- REFRESH TABLE workspace.default.bronze_camara_deputados;
```

---

### Problema: Dashboard Não Carrega Dados

**Erro:**
```
Error loading data: Table not found
```

**Solução:**
1. Verificar se as tabelas fonte existem:
```sql
SHOW TABLES IN workspace.default LIKE 'silver_camara_fact_eventos';
SHOW TABLES IN workspace.default LIKE 'gold_camara_despesas_por_tipo';
```

2. Re-executar os notebooks para gerar os dados:
   - Execute `01_Bronze_Ingestao`
   - Execute `02_Silver_Transformacao`
   - Execute `03_Gold_Analise`

3. Fazer refresh do dashboard:
   - Abra o dashboard
   - Clique no botão **"Refresh"** no canto superior direito

---

### Problema: Erro de Autenticação GitHub

**Erro ao fazer push:**
```
Authentication failed
```

**Solução:**
1. Criar GitHub Personal Access Token (PAT)
   - Acesse: https://github.com/settings/tokens
   - Scope: `repo` (completo)
2. Configurar no Databricks:
   - User Settings > Git integration
   - Git provider: GitHub
   - Token: Cole o PAT
3. Recriar Databricks Repo

---

### Problema: PySparkValueError

**Erro:**
```
[CANNOT_PARSE_JSON_FIELD] Cannot parse the field from required JSON string
```

**Solução:**
- Já tratado no código com `get_json_object()`
- Verificar se executou Bronze antes de Silver
- Re-executar célula de ingestão se necessário

---

## 🗺️ Roadmap

### Curto Prazo (Q1 2025)
- [x] Criar 2 dashboards interativos (Eventos e Despesas) ✅
- [ ] Adicionar testes unitários com `pytest`
- [ ] Implementar CI/CD com GitHub Actions
- [ ] Adicionar validação de schema com Great Expectations
- [ ] Criar mais dashboards (Engajamento, Votações, Fornecedores)

### Médio Prazo (Q2 2025)
- [ ] Implementar **incremental load** (apenas dados novos)
- [ ] Adicionar mais anos de dados históricos (2020-2023)
- [ ] Criar tabelas de auditoria (lineage, data quality)
- [ ] Implementar alertas automáticos para anomalias
- [ ] Adicionar filtros dinâmicos nos dashboards

### Longo Prazo (H2 2025)
- [ ] Migrar para Lakeflow Spark Declarative Pipelines (DLT)
- [ ] Adicionar Machine Learning (previsão de despesas)
- [ ] Criar API REST para consulta de dados
- [ ] Implementar Data Mesh com ownership por domínio
- [ ] Publicar dashboards como aplicação web pública

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Para mudanças importantes:

1. **Abra uma issue** descrevendo a mudança proposta
2. **Fork** o repositório
3. **Crie uma branch** para sua feature (`git checkout -b feature/NovaFeature`)
4. **Commit** suas mudanças (`git commit -m 'Adiciona NovaFeature'`)
5. **Push** para a branch (`git push origin feature/NovaFeature`)
6. **Abra um Pull Request**

### Diretrizes de Código
- Seguir PEP 8 para Python
- Adicionar docstrings para funções
- Escrever testes para novas features
- Atualizar README se necessário

---

## 📄 Licença

Este projeto está licenciado sob a **MIT License** - veja o arquivo [LICENSE](LICENSE) para detalhes.

```
MIT License

Copyright (c) 2025 Carlos Freitas

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
```

---

## 📧 Contato

**Autor:** Carlos Freitas

- 📧 **Email:** cafreitas@gmail.com
- 🐙 **GitHub:** [@cafreitas10](https://github.com/cafreitas10)
- 💼 **LinkedIn:** [Carlos Freitas](https://linkedin.com/in/cafreitas10)
- 🌐 **Repositório:** [github.com/cafreitas10/camara_brasil](https://github.com/cafreitas10/camara_brasil)

---

## 🔗 Referências

### Fontes de Dados
- **API Dados Abertos da Câmara:** https://dadosabertos.camara.leg.br/
- **Documentação da API:** https://dadosabertos.camara.leg.br/swagger/api.html
- **Portal da Transparência:** https://www.camara.leg.br/transparencia/

### Tecnologias
- **Databricks:** https://databricks.com/
- **Delta Lake:** https://delta.io/
- **PySpark:** https://spark.apache.org/docs/latest/api/python/
- **Unity Catalog:** https://docs.databricks.com/data-governance/unity-catalog/
- **Databricks AI/BI Dashboards:** https://docs.databricks.com/dashboards/

### Artigos e Tutoriais
- **Arquitetura Medallion:** https://www.databricks.com/glossary/medallion-architecture
- **Star Schema:** https://en.wikipedia.org/wiki/Star_schema
- **Z-score Anomaly Detection:** https://en.wikipedia.org/wiki/Standard_score
- **Data Visualization Best Practices:** https://www.databricks.com/blog/data-visualization-best-practices

---

## 🎉 Agradecimentos

- **Câmara dos Deputados** por disponibilizar a API de Dados Abertos
- **Databricks** pela plataforma de analytics moderna
- **Comunidade open-source** pelos feedbacks e contribuições

---

<div align="center">

**⭐ Se este projeto foi útil, deixe uma estrela no GitHub! ⭐**

[![GitHub stars](https://img.shields.io/github/stars/cafreitas10/camara_brasil?style=social)](https://github.com/cafreitas10/camara_brasil)
[![GitHub forks](https://img.shields.io/github/forks/cafreitas10/camara_brasil?style=social)](https://github.com/cafreitas10/camara_brasil/fork)

---

Feito com ❤️ por [Carlos Freitas](https://github.com/cafreitas10)

🏛️ **Pipeline Medallion - Câmara dos Deputados** | 2025

</div>
