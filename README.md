# Formacao-Spark-Pyspark-Fernando-Amaral


**UDEMY: Formação Spark com Pyspark : o Curso Completo [2022]**



<img src="https://www.vectorlogo.zone/logos/apache_spark/apache_spark-ar21.png" alt="MarineGEO circle logo" style="height: 250px; width:500px;"/>

Material: https://www.eia.ai/material_download

**Seção 1:** Introdução
**Seção 2:** Instalação
**Seção 3:** DataFrames e RDDs
**Seção 4:** Spark SQL
**Seção 5:** Outras fontes de dados
**Seção 6:** Criando aplicações
**Seção 7:** Machine Learning com Spark
**Seção 8:** Spark Structured Streaming
**Seção 9:** Otimização
**Seção 10:** Outros Aspectos
**Seção 11:** Construindo um  Cluster
**Seção 12:** Aula Bônus


Arquitetura: Windows 10 + VM (Oracle Virtual Box) Linux Ubuntu Desktop

## Seção 1: Introdução

### Introdução ao Spark

#### O que é Spark

O Spark é uma ferramenta de processamento de Dados, de alta perfomance, distribuído em cluster, em memória (primariamente), veloz, escalável, hdfs ou cloud, suporte a particionamento.

Replicação e Tolerância a Falha: Dados são copiados entre os nós do cluster. 

#### Arquitetura e Componentes

* Machine Learning  (Mlib)
* SQL (Spark SQL)
* Processamento em Streaming
* Processamento de  Grafos (GraphX)

**Spark SQL**: perminte ler dados tabulares de várias fontes(CSV,Json,Parquet,ORC,etc) e pode usar sintaxe SQL.

**Streaming: Spark Structured Streaming**: dados estruturados e novos registros adicionados ao final da tabela.

**Grafos acíclicos dirigidos**: Spark constrói gráficos acíclicos dirigidos.
*Acíclicos: não tem ciclo.*
*Dirigidos: tem uma direção.*

**Tungsten**: motor de execução, otimização da CPU.

##### Estrutura

* **Driver:** Inicializa *SparkSession*, solicita recursos computacionais do Cluster Manager, transforma as operações em DAGs, distribui estas pelos executers.
* **Manager:** Gerencia os recursos do  cluster. Quatro possíveis:  built-in standalone (padrão), Yarn, Mesos e Kubernetes.
* **Executer:** roda em cada nó do cluster executando as tarefas.


##### Transformações e Ações

**Data frame**: É  imutável (tolerâcia a falha), uma transformação gera um novo data frame. O processamento de transformação só ocorre quando há uma ação: *Lazy Evaluation*.





## Seção 2: Instalação

