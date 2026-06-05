# Guia de Arquitetura de Referência (Reference Architecture Guide)

Este documento descreve detalhadamente a arquitetura de processamento, as decisões de design (design decisions) e os princípios de engenharia aplicados no projeto de engenharia de dados (data engineering) no ambiente corporativo do **Databricks**. A solução foi desenvolvida seguindo rigorosamente os padrões de mercado, visando alta escalabilidade (scalability), governança estrita de dados (data governance) e eficiência financeira (FinOps).

---

## 1. Visão Geral da Arquitetura (Architecture Overview)

A plataforma de dados foi estruturada sob o padrão de arquitetura de medalhão (Medallion Architecture), operando com processamento distribuído no Apache Spark e armazenamento no formato Delta Lake:

```
[ Landing Zone (Volumes) ] 
         │
         ▼
[ Camada Bronze (Raw Delta) ] ──► Replicação Append-only e auditoria
         │
         ▼
[ Camada Silver (Cleansed Delta) ] ──► Limpeza, casting e deduplicação
         │
         ▼
[ Camada Gold (Curated Star Schema) ] ──► Tabelas Fato e Dimensões otimizadas
```

### Portabilidade Multi-Cloud (Multi-Cloud Portability)
O design de pastas do workspace Databricks foi projetado de forma totalmente desacoplada dos provedores físicos de nuvem pública (cloud providers), como AWS, Azure ou GCP. Isso foi alcançado centralizando a parametrização de caminhos físicos e esquemas através de notebooks globais de inicialização.

---

## 2. Ingestão e Estruturação de Landing Zone (Landing Zone & Raw Replication)

### Volumes no Unity Catalog (Unity Catalog Volumes)
A zona de pouso (Landing Zone) foi implementada utilizando volumes (Volumes) do Unity Catalog, eliminando a dependência de montagens manuais de diretórios (mount points) com credenciais expostas:
*   **Caminho Padronizado:** Os arquivos brutos de entrada são recebidos no caminho de volumes `/Volumes/case_databricks/landing/volume_landing/case/`.
*   **Organização Semântica:** Cada origem de dados é depositada em subdiretórios específicos para cada domínio (ex: `crm_clientes`, `cadastro_produtos_api_dump`, `logistica_entregas`), facilitando a governança e a segurança de acesso aos dados.

### Ingestão via Auto Loader (Auto Loader Ingestion)
Para mover os dados da Landing para a camada Bronze, adotou-se o **Databricks Auto Loader** (`cloudFiles`):
*   **Schema Evolution:** O Auto Loader detecta automaticamente mudanças estruturais nos arquivos de entrada sem quebrar o processamento.
*   **Replicação Raw e Auditoria:** Os dados são salvos em tabelas Delta idênticas às origens (append-only), acrescidas de metadados técnicos de rastreabilidade:
    *   `rastreamento_source`: Caminho físico do arquivo processado para auditoria.
    *   `ingestion_date_brasilia`: Carimbo de data/hora (timestamp) no fuso horário local de Brasília.

---

## 3. Qualidade e Higienização de Dados (Silver Layer & Data Quality)

A camada Silver (Cleansed) é responsável por garantir a higienização dos dados cadastrais e o cumprimento de contratos de dados (data contracts) corporativos antes da modelagem analítica final.

### Processo de Limpeza e Padronização (Data Cleaning Standards)
Cada pipeline de dados (data pipeline) aplica transformações e regras de qualidade (quality rules) específicas na Silver:
*   **Higienização de Identificadores:** CPFs e CNPJs são limpos utilizando expressões regulares (regex) para remover caracteres de formatação (pontos, traços e barras).
*   **Conversão de Tipos (Casting):** Strings monetárias e preços são convertidos para decimais de precisão fixa (`DecimalType(10, 2)`), e strings de data e hora para timestamps (TimestampType).
*   **Deduplicação Inteligente (Windowing Deduplication):** Para lidar com dados mutáveis e dumps incrementais (como status de pedidos), utiliza-se a janela de partição analítica do Spark (`Window.partitionBy().orderBy()`) mantendo apenas o registro correspondente ao estado mais atualizado (latest state).

---

## 4. Modelagem Dimensional Analítica (Gold Layer & Star Schema)

A camada Gold (Curated/BI) foi estruturada sob o conceito de **esquema estrela (star schema)**, segregando os dados em tabelas de dimensão (dimension tables) e tabelas fato (fact tables), facilitando consultas imediatas por ferramentas de Business Intelligence (BI) e pelo **Databricks AI/BI Genie** (consultas em linguagem natural).

### Integridade Referencial com Chaves Substitutas (Surrogate Keys)
*   **SHA-256 para Chaves de Negócio:** Em conformidade com as boas práticas de modelagem multidimensional moderna, as dimensões utilizam chaves substitutas (surrogate keys) calculadas via hash SHA-256 sobre a concatenação das chaves naturais. Isso isola o modelo analítico do ID de banco de dados transacional e permite cargas incrementais paralelas sem gerar chaves sequenciais auto-incrementais que quebram o paralelismo.
*   **Tratamento de Registros Órfãos (Fallback Key Routing):** Se durante o enriquecimento das tabelas fato for identificada uma chave estrangeira órfã (sem correspondência na tabela de dimensão), o pipeline realiza um roteamento automático para um ID padrão de fallback (`-1` ou hash de "Não Identificado"). Isso garante a **integridade referencial** sem o risco de descartar transações financeiras e métricas críticas.

---

## 5. Performance e Eficiência Computacional (Spark Optimization & FinOps)

A excelência arquitetural foi implementada adotando otimizações avançadas focadas em desempenho de consultas (query performance) e redução de desperdício financeiro de clusters (cloud cost management):

### Liquid Clustering (Clustering Dinâmico)
As tabelas físicas Delta da camada Gold utilizam **Liquid Clustering** (`CLUSTER BY`) em substituição ao particionamento tradicional (diretórios físicos por colunas). 
*   **Por que (Why):** Evita o problema de pequenos arquivos (small files problem), acelera drasticamente as consultas contendo filtros dinâmicos de BI (ex: filtragem regional por UF ou filtros de data) e otimiza a gravação incremental.

### Paralelização concorrente de Cargas (Concurrent Notebook Execution)
Para acelerar o processamento das camadas Silver e Gold, a orquestração realiza a execução concorrente de notebooks:
*   **Implementação:** Utiliza-se um orquestrador baseado em threads (`threading.Thread`) combinadas com filas thread-safe (`queue.Queue`) e a chamada nativa `dbutils.notebook.run()`.
*   **Resultado:** Permite que notebooks independentes rodem em paralelo compartilhando a mesma sessão do Spark, reduzindo o tempo ocioso do cluster e acelerando o tempo de processamento.

### Filtro Antecipado e Legibilidade (Predicate Pushdown & CTEs)
A escrita de códigos SQL e Spark adota os princípios de otimização de plano lógico de execução:
*   **Predicate Pushdown:** Os pipelines aplicam filtros e partições logo no início das leituras das tabelas de origem, reduzindo o volume de dados trafegados (network shuffle) nas operações de junção (JOIN).
*   **Common Table Expressions (CTEs):** Substitui subconsultas aninhadas por blocos CTE (`WITH`) nomeados de forma semântica, melhorando drasticamente a manutenção do código.

---

## 6. Próximos Passos de Evolução (Future Modifications)

Como parte do ciclo de vida de desenvolvimento de software (SDLC) e melhoria contínua, as seguintes evoluções de arquitetura são recomendadas:

1.  **Migração para Delta Live Tables (DLT):** Conversão dos fluxos imperativos do Auto Loader para pipelines declarativos do DLT, permitindo o gerenciamento automático de dependências de tabelas e expectativas de qualidade (Expectations).
2.  **Monitoramento Ativo de Qualidade de Dados (Data Observability):** Implementação de contratos de dados automáticos com ferramentas como Great Expectations ou Soda, gerando alertas imediatos via webhooks de notificação em caso de anomalia estrutural ou volumétrica.
3.  **Linhagem Automática no Unity Catalog (Unity Catalog Lineage):** Habilitar a linhagem automática de dados do Unity Catalog para rastreamento de ponta a ponta, desde a Landing até a Gold, garantindo conformidade regulatória (LGPD/GDPR) e simplificando análises de impacto em alterações de colunas.
