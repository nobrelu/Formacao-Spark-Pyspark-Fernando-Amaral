# Formacao-Spark-Pyspark-Fernando-Amaral


**UDEMY: Formação Spark com Pyspark : o Curso Completo [2022]**



<img src="https://www.vectorlogo.zone/logos/apache_spark/apache_spark-ar21.png" alt="Spark" style="height: 250px; width:500px;"/>

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


______________________________

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

______________________________

## Seção 2: Instalação

**Criação da Máquina Virtual:**

Tipo: Linux
Versão:  Ubuntu (64-bit)
Memória base: 14747 MB (min. 4)
Processadores: 5 CPUs (min. 1)
Disco rígido:  novo, a partir de 25 GB, tipo VDI

**Configurações extras:**

Monitor > Memória de  Vídeo:  80 (min. 32) 
Armazenamento > Ascrecentar imagem do ubuntu baixada (ubuntu-22.04.1-desktop-amd64.iso) para o curso em  Controladora: IDE. E remove a vazia.
Geral > Ativados área de transferência compartilhada e arrastar e soltar.
Rede >  Conectado a NAT

Instalação da imagem ubuntu na  VM

Instalação do java: sudo apt install curl mlocate default-jdk -y

Instalação do spark e  ajustes de variaveis em ~/.bashrc

**Inicialização do spark:**
 start-master.sh (em standalone)
/opt/spark/sbin/start-slave.sh  spark://localhost:7077  (worker)


**Acessar spark:**
spark-shell (na linguagem scala)
:quit
pyspark 

Bibliotecas adicionais do Python:
sudo apt install python3-pip

pip install numpy
pip install pandas


**Habilitar SSH  na Máquina Virtual: **
sudo apt update
sudo apt install openssh-server

Configurar as portas - desligar a VM para editar as configurações:
Rede > Avançado > Redirecionamento de portas > Adicionar > 
Nome: SSH
Protocolo: TCP
End. IP Hospedeiro: 127.0.0.1
Porta do Hospedeiro e do Convidado: 22

ip a - valida ip registrado

**Utilização do WinSSH e do WinSCP**


______________________________


## Seção 3: DataFrames, Dataset e RDD's

### RDD - Resilient Distributed Datasets 

Será estudado no curso, porém não é o foco.

* Estrutura básica de baixo nível
* Dados "imutáveis", distribuídos pelo cluster
* Em memória
* Pode ser persistindo em disco*
* Tolerante a falha
* Operações sobre um RDD criam um novo RDD
* Complexo e verboso
* Otimização difícil pelo Spark

### Dataset e DataFrame

* Semelhante a uma tabela de banco de dados
* Compatívelcom objetos dataframe do  R e  Python

**Dataset**:  *não terá no curso* 
*  Disponível em Java  e  Scala
*  Não disponível em R e Python

**Data Frame**:  *Será prioridade do curso*


#### RDD - parte 1

Iniciar pyspark: <code>pyspark</code>
Criação de váriavel:

> numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
> numeros.take(5) ->  [1, 2, 3, 4, 5]
> numeros.top(5) -> [10, 9, 8, 7, 6]
> numeros.collect() -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


Entre outros:

> numeros.mean()
> numeros.sum()
> numeros.max()
> numeros.min()
> numeros.stdev()
> numeros.count()


E filtros:

> filtro = numeros.filter(lambda filtro: filtro >  2)
> filtro.collect()

> amostra  = numeros.sample(True,0.5,1)
> amostra.collect()

> mapa = numeros.map(lambda mapa: mapa * 2)
> mapa.collect()


> numeros2 = sc.parallelize([6,7,8,9,10])
> uniao = numeros.union(numeros2)
> uniao.collect()
> -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 6, 7, 8, 9, 10]

> interseccao  =  numeros.intersection(numeros2)
> interseccao.collect()
> -> [10, 6, 7, 8, 9]

> subtrai = numeros.subtract(numeros2)
> subtrai.collect()
> -> [1, 2, 3, 4, 5]

> cartesiano = numeros.cartesian(numeros2)
> cartesiano.collect()
> ->  [(1, 6), (2, 6), (1, 7), (2, 7), (1, 8), (2, 8), (1, 9), (2, 9), (1, 10), (2, 10), (3, 6), (4, 6), (3, 7), (4, 7), (3, 8), (4, 8), (3, 9), (4, 9), (3, 10), (4, 10), (5, 6), (6, 6), (5, 7), (6, 7), (5, 8), (6, 8), (5, 9), (6, 9), (5, 10), (6, 10), (7, 6), (8, 6), (7, 7), (8, 7), (7, 8), (8, 8), (7, 9), (8, 9), (7, 10), (8, 10), (9, 6), (10, 6), (9, 7), (10, 7), (9, 8), (10, 8), (9, 9), (10, 9), (9, 10), (10, 10)]
> 
> cartesiano.countByValue()
> -> defaultdict(<class 'int'>, {(1, 6): 1, (2, 6): 1, (1, 7): 1, (2, 7): 1, (1, 8): 1, (2, 8): 1, (1, 9): 1, (2, 9): 1, (1, 10): 1, (2, 10): 1, (3, 6): 1, (4, 6): 1, (3, 7): 1, (4, 7): 1, (3, 8): 1, (4, 8): 1, (3, 9): 1, (4, 9): 1, (3, 10): 1, (4, 10): 1, (5, 6): 1, (6, 6): 1, (5, 7): 1, (6, 7): 1, (5, 8): 1, (6, 8): 1, (5, 9): 1, (6, 9): 1, (5, 10): 1, (6, 10): 1, (7, 6): 1, (8, 6): 1, (7, 7): 1, (8, 7): 1, (7, 8): 1, (8, 8): 1, (7, 9): 1, (8, 9): 1, (7, 10): 1, (8, 10): 1, (9, 6): 1, (10, 6): 1, (9, 7): 1, (10, 7): 1, (9, 8): 1, (10, 8): 1, (9, 9): 1, (10, 9): 1, (9, 10): 1, (10, 10): 1})





#### RDD - parte 2


compras = sc.parallelize([
    (1,200),
    (2,300),
    (3,120),
    (4,250),
    (5,78)    
    ])


chaves =  compras.keys()
chaves.collect()

valores = compras.values()
valores.collect()

compras.countByKey()

soma = compras.mapValues(lambda soma: soma + 1)
soma.collect()

debitos = sc.parallelize([
    (1,20),
    (2,300)
])

--inner join

resultado = compras.join(debitos)
resultado.collect()

semdebito =  compras.subtractByKey(debitos)
semdebito.collect()

#### Data Frame - parte 1



#### Data Frame - parte 2




#### Data Frame - parte 3




#### Data Frame - parte 4