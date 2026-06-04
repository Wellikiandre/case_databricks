# Arquitetura Corporativa de Dados & Boas Práticas (Databricks Reference Architecture)

> [!IMPORTANT]
> **Premissa de Entrega & Posicionamento Profissional**
> 
> A solução desenvolvida para este teste técnico está implementada integralmente sob a arquitetura de referência detalhada a seguir. 
> 
> Mesmo ciente de que o foco central deste case é a avaliação de minhas habilidades técnicas (skills evaluation), assumi como premissa pessoal a entrega de **valor incremental (incremental value)**. Por este motivo, meu objetivo aqui não foi apenas documentar estritamente as regras básicas solicitadas pelo enunciado, mas sim construir e documentar o projeto sob o mesmo padrão de excelência de mercado que venho aplicando, liderando e ensinando em grandes operações de dados há anos.

Este documento detalha o guia de arquitetura de referência (Reference Architecture Guide) projetado para o ambiente corporativo do **Databricks**. Esta arquitetura foi consolidada sob os princípios de alta escalabilidade (scalability), governança estrita de dados (data governance) e otimização financeira (FinOps), sendo testada com sucesso em grandes corporações do mercado financeiro e de tecnologia.

---

## 1. Diretrizes Iniciais de Acesso e Ingestão

### Controle de Acesso Baseado em Perfis (Access Control & Roles)
*   **Grupos de Usuários (User Groups):** Implementação de políticas de controle de acesso baseado em funções (Role-Based Access Control - RBAC). Os usuários são segregados em grupos com privilégios específicos: *Analistas de BI*, *Engenheiros de Dados*, *Analistas de Governança* e *Membros do Centro de Excelência (Center of Excellence - CoE)*.
*   **Segregação por Unidade de Negócio (Business Unit - BU):** Isolamento de dados entre diferentes BUs para garantir privacidade, conformidade regulatória e simplificação do rateio de custos de nuvem.

### Estrutura de Pastas na Zona de Ingestão (Landing Zone)
Em ambientes de produção de alta escala, a organização física e lógica na zona de pouso (Landing Zone) deve ser padronizada por fonte e partição temporal, otimizando o paralelismo de leitura do Spark e facilitando a governança:

+```text
+sistema / fonte (tabela ou endpoint) / ano / mês / dia / formato_arquivo
+```
---

## 2. Estrutura de Pastas do Projeto (Workspace Folder Structure)

A estrutura abaixo representa o padrão arquitetural de pastas adotado em grandes *players* de mercado nos três principais provedores de nuvem (AWS, GCP e Azure). O design é totalmente desacoplado da nuvem física, permitindo portabilidade e adaptabilidade a diferentes domínios de negócio:

![Diagrama de Arquitetura da Solução](arquitetura.png)

 Pasta | Componente | Descrição Técnica & Objetivo de Engenharia |
 :--- | :--- | :--- |
 **`0_Config`** | Configurações & Inicialização | Contém o notebook `init` que centraliza o carregamento de dependências, bibliotecas (libraries), funções utilitárias compartilhadas e parametrização dinâmica de ambientes (DEV, HML, PRD). É o cérebro e ponto único de controle do ecossistema. |
 **`1_Landing`** | Entrada de Dados (Landing) | Ponto de contato inicial com as origens brutas. Configurado para ler tópicos (topics) de mensageria, eventos de captura de mudança de dados (Change Data Capture - CDC), extrações de API ou cargas em lote (batch). |
 **`2_Bronze`** | Camada Bronze (Raw Delta) | Replicação exata dos dados de origem em tabelas delta (Delta Tables). Preserva o histórico bruto (append-only) e adiciona metadados de auditoria (ex: data e hora de inserção - timestamp). |
 **`3_Silver`** | Camada Silver (Cleansed Delta) | Aplicação de regras de qualidade, conversão de tipos (casting), normalização e padronização de nomenclatura de colunas (schema alignment), deduplicação e enriquecimento de dados. |
 **`4_Gold`** | Camada Gold (Curated/BI) | Modelagem analítica final otimizada para o consumo de dashboards de BI. Suporta modelagem dimensional (Tabelas Fato e Dimensão no padrão Star Schema de Ralph Kimball), modelagem de Bill Inmon, ou o uso de tabelas consolidadas (One Big Table - OBT). |
 **`5_Workflow`** | Orquestração & Pipelines | Armazena notebooks estruturados para a automação de fluxos ponta a ponta e interfaces com orquestradores externos de pipeline. |
 **`6_Webhook`** | Notificações & Alertas | Implementação de webhooks para o envio proativo de status de saúde das cargas e alertas de falhas em tempo real (real-time notification) para plataformas de comunicação como Microsoft Teams ou Slack. |
 **`7_Vacuum_Optimize`** | Manutenção & Performance | Automação periódica de processos de otimização de tabelas Delta (`OPTIMIZE` e `VACUUM`), reduzindo fragmentação de arquivos e limpando logs transacionais antigos para manter a eficiência de leitura (query performance). |
 **`8_FinOps`** | Finanças na Nuvem (FinOps) | Scripts especializados na análise de uso de clusters, eficiência de consultas e redução de desperdício financeiro na nuvem. |
 **`9_Governanca`** | Governança & Segurança | Cadernos dedicados ao mascaramento de dados sensíveis (data masking), controle de integridade e aderência às regras de conformidade (LGPD/GDPR). |

---

## 3. Governança Moderna & Prontidão para IA (Unity Catalog & Genie AI)

### Repositório de Metadados Centralizado (Centralized Metadata Repository)
Conforme as melhores práticas de governança moderna, o local ideal e definitivo da documentação técnica de dados (como tipos de dados, descrições de colunas e dicionários) é **dentro do Unity Catalog** do Databricks, garantindo governança centralizada e rastreabilidade através de linhagem de dados (Data Lineage). 
O arquivo `doc.md` serve como o documento de arquitetura e design do projeto, contudo, para fins de demonstração neste case técnico, disponibilizamos exemplos descritivos integrados para ilustrar como os metadados são documentados.

### Modelagem Orientada a IA (AI-Ready Data & Databricks Genie)
Como premissa fundamental de design, as tabelas finais da camada Gold foram totalmente projetadas e otimizadas para consumo direto do **Databricks AI/BI Genie** (nossa ferramenta de análise conversacional de dados). Isso permite que usuários de negócio realizem perguntas em linguagem natural (Natural Language) diretamente para os dados e recebam respostas imediatas. A engenharia do modelo de dados adotou as seguintes diretrizes para garantir essa prontidão para IA (AI-Ready):
*   **Semântica Autoexplicativa:** Nomes de colunas e tabelas intuitivos que dispensam traduções complexas ou decodificações por parte do modelo de linguagem.
*   **Metadados Ricos (Rich Meta):** Inserção de descrições detalhadas e comentários ricos diretamente no catálogo de dados (Data Catalog) para cada tabela e coluna.
*   **Estrutura Relacional Declarada:** Definição explícita de restrições de chaves primárias (Primary Keys) e chaves estrangeiras (Foreign Keys) na camada Gold, servindo de contexto contextual indispensável para a inteligência artificial interpretar os relacionamentos do negócio.

---

## 4. Destaques Arquiteturais & Otimizações de Custo Comprovadas

> [!TIP]
> **Eficiência em Orquestração: Arquitetura Orientada a Eventos (ADF & Databricks)**
> 
> A integração padrão de mercado onde o Azure Data Factory (ADF) aguarda ativamente a finalização de jobs no Databricks gera altos custos de Unidades de Integração de Dados (Data Integration Units - DIUs).
> 
> **A Solução:** Ao configurar o ADF para disparar uma chamada de API assíncrona para o notebook da pasta `5_Workflow` e liberar a execução local da pipeline imediatamente, as ferramentas operam de forma independente. Cada ferramenta atua sob sua própria responsabilidade arquitetural, **reduzindo os custos de processamento de dados do ADF entre 27% e 90%** (variando de acordo com o volume de dados e o tempo de execução do job no cluster).
> 
> [Veja o post com a explicação técnica no LinkedIn](https://www.linkedin.com/feed/update/urn:li:activity:7084565386251112449/)

> [!IMPORTANT]
> **Monitoração Proativa e Operações Eficientes**
> 
> A implementação da camada `6_Webhook` elimina a necessidade de manter analistas dedicados monitorando consoles de agendamento (schedulers) 24/7. Os alertas automáticos de quebra de contrato de dados (data contract breaches) ou falhas críticas são enviados diretamente aos canais do Teams/Slack para rápida atuação.

---

## 5. Portfólio de Casos Reais de Sucesso (FinOps & Performance Cases)

Abaixo estão detalhados os resultados práticos obtidos com a aplicação desta mesma arquitetura e de metodologias avançadas de FinOps no ecossistema de dados, servindo de base de conhecimento para o ambiente corporativo:

1.  **FinOps e Automação no Databricks (Redução de 45.5%):**
    Implementação de rotinas automatizadas e gerenciamento inteligente de clusters na zona de entrega (Delivery Zone), otimizando o gasto computacional de processamento de big data.
    *   [Acesse o Artigo Completo no LinkedIn](https://www.linkedin.com/pulse/automa%C3%A7%C3%A3o-e-finops-economia-de-455-databricks-um-caso-wellikiandre-smopf/)
2.  **Redução de 94% em Custos de Operações de Leitura/Escrita (Data Lake):**
    Otimização de rotinas de leitura e gravação em disco através do ajuste do tamanho de partição de arquivos, prevenção do problema de arquivos pequenos (Small File Problem) e eliminação de leituras desnecessárias de dados.
    *   [Acesse o Artigo Completo no LinkedIn](https://www.linkedin.com/pulse/redu%C3%A7%C3%A3o-de-custo-data-lake-operation-readwhite-wellikiandre/?trackingId=wUfs%2BfJERQa%2B%2B7RzHkscfA%3D%3D)
3.  **Otimização de Carga no Power BI (De 21 minutos e 57 GiB para < 1 minuto e 1.2 GiB):**
    Aceleração dramática na atualização de painéis corporativos aplicando agregação antecipada de dados na camada Gold (Star Schema), reduzindo o volume trafegado (network shuffle) e o consumo de memória RAM do Gateway de dados.
    *   [Acesse a Publicação com Detalhes Técnicos](https://www.linkedin.com/posts/wellikiandre_como-reduzi-57gib-de-dados-por-ciclo-de-activity-7211750188825124864-YCE2?utm_source=share&utm_medium=member_desktop&rcm=ACoAACVzuN0B2yWsXoXg_wqXapwdWiXb-zH4_4U)
4.  **Monitoramento Automatizado de Pipelines com Assistente Webhook:**
    Arquitetura de notificação integrada aos cadernos da camada `6_Webhook` para alertas automáticos de jobs, eliminando a verificação manual constante.
    *   [Acesse a Publicação no LinkedIn](https://www.linkedin.com/feed/update/urn:li:activity:7110645408216813568/)
5.  **Ajustes Finos de Performance em Processamento de Fluxo Contínuo (Streaming Tuning):**
    Melhorias aplicadas a fluxos estruturados (Structured Streaming) para estabilização de vazão de dados (throughput) e redução de latência no Databricks.
    *   [Acesse a Publicação no LinkedIn](https://www.linkedin.com/feed/update/urn:li:activity:7218722493941878784/)
6.  **Ingestão de IoT Near Real-Time Otimizada computacionalmente:**
    Consumo resiliente de sensores com o menor consumo computacional necessário através da otimização de gatilhos (triggers) de processamento de stream do Spark.
    *   [Acesse a Publicação no LinkedIn](https://www.linkedin.com/feed/update/urn:li:activity:7023771647023144960/?updateEntityUrn=urn%3Ali%3Afs_feedUpdate%3A%28V2%2Curn%3Ali%3Aactivity%3A7023771647023144960%29)