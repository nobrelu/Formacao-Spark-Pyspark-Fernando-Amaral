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


**Lazy Evalution**:  

**Transformações:**
Filter, Union, Sample, map, flatMap, mapPartititions, mapPartititionsWithIndex, intersection, distinct, groupByKey, reduceByKey, aggregateByKey, sortByKey, join, cogroup, cartesian, pipe, coalesce, repartition, repartitionAndSortWithinPartitions

**Ação:**
show, reduce, collect, count, first, take, takeSample, takeOrdered, saveAsTextFile, saveAsSequenceFile, saveAsObjectFile, countByKey, foreach
Dessa forma, o Spark consegue otimizar o processo e criar uma plano para as transformações.

**Tipos de transformações:**
* Narrow: os dados estão em uma mesma partição
* Wide:  os dados estão em mais de uma partição




##### Componentes

**Job**: Tarefa
**Stage**: Divisão do Job
**Task**:  menor unidade de trabalho. Uma por núcleo e por partição.



#### Context e Session

##### SparkContext

Conexão transparente com o Cluster.

##### SparkSession

Acesso ao SparkContext.

Possibilidade de rodar script Spark no shell (pyspark).
O Spark cria uma sessão automaticamente chamada spark.
Para criar uma aplicação sparm, você precisa criar:



<code>spark  = (SparkSession
.builder
.appName("Meuapp")
.getOrCreate()) </code>

#### Formatos de Big Data

Formatos desacoplados de ferramentas, diários, compactados, suportam schema e podem ser particionados entre discos (redundância e paralelismo).


*  Parquet: colunar, padrão do spark - Melhor perfomance na consulta (leitura)
*  Avro: linha
*  Orc: colunar, padrão do hive - Em geral é mais eficiente na criação (escrita)  e na compreessão

Linha:  muitos atributos e mais escrita
Coluna: menos atributos e mais leitura
