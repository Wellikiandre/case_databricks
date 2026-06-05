# Documentação Técnica da Solução (Technical Documentation)

Esta documentação técnica (technical documentation) descreve a arquitetura de processamento, as decisões de design (design decisions) e o modelo de dados (data model) implementado no Databricks para o case de engenharia de dados.

---

## 1. Visão Geral da Arquitetura (Architecture Overview)

A solução foi desenvolvida baseando-se na arquitetura de medalhão (Medallion Architecture) adaptada para operar no Databricks de forma resiliente e escalável:

1.  **Camada Landing**: Ponto de contato onde os arquivos brutos em lote (batch) ou fluxo contínuo (streaming) residem. A zona de pouso (Landing Zone) foi estruturada de forma padronizada utilizando volumes (Volumes) do Unity Catalog no Databricks, sob o caminho `/Volumes/case_databricks/landing/volume_landing/case/`, organizando as origens de dados em subdiretórios específicos para cada entidade de negócio.

    ![Estrutura da Camada Landing](../volume_landing.png)
2.  **Camada Bronze**: Tabela delta (Delta Table) que armazena a réplica exata das origens de dados (append-only) com o acréscimo de metadados de auditoria (`rastreamento_source` e `ingestion_date_brasilia`).
3.  **Camada Silver**: Tabelas tratadas, limpas e deduplicadas, com esquemas alinhados (schema alignment) e tipos de dados convertidos (casting) adequadamente.
4.  **Camada Gold**: Modelo de dados analítico estruturado sob o conceito de esquema estrela (star schema), composto de tabelas de dimensão (dimension tables) e tabelas fato (fact tables).

---

## 2. Regras de Qualidade e Tratamento (Data Quality & Cleaning)

Para cada entidade, aplicou-se um pipeline de limpeza (cleaning pipeline) específico na camada Silver:

*   **Clientes CRM (`crm_clientes`)**:
    *   Limpeza do documento (CPF/CNPJ) através de expressões regulares (regex) para remover caracteres de formatação (como pontos e traços).
    *   Padronização do campo e-mail em letras minúsculas (lowercase).
    *   Tratamento de duplicidade através do particionamento por `id_cliente` no nível do banco de dados (database deduplication).
*   **Produtos (`cadastro_produtos`)**:
    *   Casting de `list_price` para formato numérico de precisão fixa (`DecimalType(10, 2)`).
    *   Tratamento de nulos em campos de categorias aplicando valor padrão "Outros" para evitar distorções na segmentação de relatórios.
    *   Deduplicação baseada no produto mais recentemente atualizado com a janela do Spark (`Window.partitionBy("product_id").orderBy(col("updated_at").desc())`).
*   **Canais e Vendedores**:
    *   Normalização de chaves primárias e chaves estrangeiras (foreign keys) para tipos de dados inteiros comuns.
    *   Ajuste de strings (remover espaços sobressalentes).
*   **Logística de Entregas (`logistica_entregas`)**:
    *   Casting das strings de tempo para tipo Timestamp.
    *   Tratamento de nulos no status do rastreamento preenchendo com "PENDENTE".
*   **Atendimento Ocorrências (`atendimento_ocorrencias`)**:
    *   Casting de datas de criação dos tickets.
    *   Filtro e deduplicação de ocorrências concorrentes por `ticket_id`, mantendo a última atualização registrada.

---

## 3. Modelo Dimensional Gold (Gold Dimensional Modeling)

Para facilitar a exploração analítica por ferramentas de Business Intelligence (BI) e pelo Databricks Genie AI, a camada Gold foi modelada em **Star Schema**:

### Tabelas de Dimensão (Dimension Tables)

#### `dim_clientes`
*   **sk_cliente (Primary Key)**: Chave substituta (surrogate key) gerada via hash SHA-256 no ID do cliente para isolamento do ID transacional.
*   **id_cliente**: ID de negócio original do cliente.
*   **nome_cliente**: Nome completo.
*   **email_cliente**: E-mail tratado.
*   **documento_cliente**: CPF/CNPJ higienizado.
*   **tipo_documento_cliente**: Classificação do documento.
*   **uf_cliente**: Unidade Federativa.
*   **regiao_cliente**: Região comercial unificada a partir dos dados geográficos do legado.
*   **data_cadastro**: Data de ingresso do cliente.

#### `dim_produtos`
*   **sk_produto (Primary Key)**: Hash SHA-256 sobre o ID do produto.
*   **id_produto**: ID de negócio do produto.
*   **nome_produto**: Descrição do produto.
*   **categoria_produto**: Categoria de alto nível.
*   **subcategoria_produto**: Subcategoria.
*   **status_produto**: Situação atual do produto.
*   **preco_tabela**: Preço de tabela sugerido.
*   **moeda**: Código de moeda.

#### `dim_vendedores`
*   **sk_vendedor (Primary Key)**: Hash SHA-256 sobre o ID do vendedor.
*   **id_vendedor**: ID numérico.
*   **nome_vendedor**: Nome do vendedor.
*   **nome_canal**: Nome do canal comercial ao qual o vendedor pertence.
*   **email_vendedor**: E-mail do vendedor.

#### `dim_tempo`
*   **sk_tempo (Primary Key)**: Chave inteira (surrogate key) no formato `yyyyMMdd`.
*   **data**: Tipo Date.
*   **ano**, **mes**, **dia**, **trimestre**, **dia_semana**, **nome_mes**, **nome_dia_semana**: Atributos temporais ricos.

### Tabelas Fato (Fact Tables)

#### `fact_pedidos_itens`
Contém as transações de vendas no nível mais granular (item por pedido).
*   **id_fato_item_pedido (Primary Key)**: Hash SHA-256 composto pela junção de pedido e produto.
*   **id_pedido**: Número do pedido.
*   **sk_cliente**, **sk_produto**, **sk_vendedor**, **sk_tempo**: Chaves estrangeiras associadas às tabelas de dimensão.
*   **quantidade**: Volume físico vendido.
*   **preco_unitario**: Preço praticado na venda.
*   **valor_bruto**: Receita bruta (`quantidade * preco_unitario`).
*   **valor_liquido**: Receita líquida ajustada (valor zerado no caso de pedidos cancelados, servindo como métrica financeira confiável).
*   **status_pedido_evento**: Status de finalização do pedido.

#### `fact_entregas`
Métricas de entrega da cadeia de suprimentos (supply chain).
*   **id_fato_entrega (Primary Key)**: ID único da entrega.
*   **id_pedido**: ID do pedido de origem.
*   **sk_cliente**: Chave do cliente destinatário.
*   **sk_tempo_envio**, **sk_tempo_entrega**: Conexões com a dimensão tempo.
*   **status_entrega**: Status do tráfego (ex: Entregue, Cancelado, Atrasado).
*   **transportadora**: Parceiro logístico.
*   **modalidade_transporte**: Modal.
*   **custo_frete**: Custo de envio logístico.
*   **dias_transporte**: Tempo decorrido em trânsito.
*   **flag_atrasado**: Indicador binário (`1` para atrasado, `0` para no prazo).

#### `fact_ocorrencias`
Monitoramento de pós-venda e satisfação do cliente.
*   **id_fato_ticket (Primary Key)**: ID do ticket.
*   **id_pedido**: Pedido associado ao chamado.
*   **sk_cliente**: Cliente que abriu a ocorrência.
*   **sk_tempo_ocorrencia**: Data de abertura do incidente.
*   **tipo_evento**: Categoria da ocorrência.
*   **severidade**: Impacto.
*   **status_ticket**: Situação de resolução.

---

## 4. Decisões de Modelagem e Performance (Design Decisions)

1.  **Liquid Clustering**: As tabelas foram projetadas para utilizar o Liquid Clustering (clustering dinâmico) do Databricks em substituição ao particionamento tradicional por colunas estáticas (como `ano`/`mes`). Isso otimiza o desempenho de leitura de filtros dinâmicos de BI (ex: filtragem regional ou temporal) e elimina o risco de problemas de arquivos pequenos (small files problem).
2.  **Chaves Substitutas via SHA-256**: Adotou-se chaves substitutas hash de 256 bits para as dimensões. Isso garante integridade em arquiteturas desacopladas e permite processar cargas incrementais de dimensões de forma isolada, evitando chaves sequenciais mutáveis.
3.  **Junção Filtre Cedo, Una Tarde (Predicate Pushdown)**: Os notebooks foram desenhados para filtrar registros inválidos ou nulos nas origens antes de executar as junções de enriquecimento, minimizando a quantidade de dados em tráfego (network shuffle) nas operações de JOIN.

---

## 5. Próximos Passos de Evolução (Future Evolutions)

*   **Unity Catalog Lineage**: Integração automática dos metadados da Gold com a linhagem automática de dados (auto-data-lineage) do Databricks Unity Catalog para auditoria e governança estrita de ponta a ponta.
*   **Delta Live Tables (DLT)**: Migração dos scripts batch/streaming do Auto Loader para pipelines nativos do DLT com regras declarativas de qualidade de dados (Expectations).
*   **Monitoramento de Qualidade com Soda/Great Expectations**: Implementação de testes automatizados de sanidade na camada Silver para capturar desvios de formato antes que eles afetem os dashboards de BI da Gold.
