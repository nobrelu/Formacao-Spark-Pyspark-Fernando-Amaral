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

    numeros = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    numeros.take(5) ->  [1, 2, 3, 4, 5]
    numeros.top(5) -> [10, 9, 8, 7, 6]
    numeros.collect() -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


Entre outros:

    numeros.mean()
    numeros.sum()
    numeros.max()
    numeros.min()
    numeros.stdev()
    numeros.count()


E filtros:

    filtro = numeros.filter(lambda filtro: filtro >  2)
    filtro.collect()

    amostra  = numeros.sample(True,0.5,1)
    amostra.collect()

    mapa = numeros.map(lambda mapa: mapa * 2)
    mapa.collect()


    numeros2 = sc.parallelize([6,7,8,9,10])
    uniao = numeros.union(numeros2)
    uniao.collect()
    -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 6, 7, 8, 9, 10]

    interseccao  =  numeros.intersection(numeros2)
    interseccao.collect()
    -> [10, 6, 7, 8, 9]

    subtrai = numeros.subtract(numeros2)
    subtrai.collect()
    -> [1, 2, 3, 4, 5]


    cartesiano = numeros.cartesian(numeros2)
    cartesiano.collect()
    ->  [(1, 6), (2, 6), (1, 7), (2, 7), (1, 8), (2, 8), (1, 9), (2, 9), (1, 10), (2, 10), (3, 6), (4, 6), (3, 7), (4, 7), (3, 8), (4, 8), (3, 9), (4, 9), (3, 10), (4, 10), (5, 6), (6, 6), (5, 7), (6, 7), (5, 8), (6, 8), (5, 9), (6, 9), (5, 10), (6, 10), (7, 6), (8, 6), (7, 7), (8, 7), (7, 8), (8, 8), (7, 9), (8, 9), (7, 10), (8, 10), (9, 6), (10, 6), (9, 7), (10, 7), (9, 8), (10, 8), (9, 9), (10, 9), (9, 10), (10, 10)]


    cartesiano.countByValue()
    -> defaultdict(<class 'int'>, {(1, 6): 1, (2, 6): 1, (1, 7): 1, (2, 7): 1, (1, 8): 1, (2, 8): 1, (1, 9): 1, (2, 9): 1, (1, 10): 1, (2, 10): 1, (3, 6): 1, (4, 6): 1, (3, 7): 1, (4, 7): 1, (3, 8): 1, (4, 8): 1, (3, 9): 1, (4, 9): 1, (3, 10): 1, (4, 10): 1, (5, 6): 1, (6, 6): 1, (5, 7): 1, (6, 7): 1, (5, 8): 1, (6, 8): 1, (5, 9): 1, (6, 9): 1, (5, 10): 1, (6, 10): 1, (7, 6): 1, (8, 6): 1, (7, 7): 1, (8, 7): 1, (7, 8): 1, (8, 8): 1, (7, 9): 1, (8, 9): 1, (7, 10): 1, (8, 10): 1, (9, 6): 1, (10, 6): 1, (9, 7): 1, (10, 7): 1, (9, 8): 1, (10, 8): 1, (9, 9): 1, (10, 9): 1, (9, 10): 1, (10, 10): 1})





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
Tabelas com linhas e colunas
Imutáveis
Com schema conhecido
Linhagem preservada
Colunas podem ter tipos diferentes
Existem análises comuns: Agrupar, ordenar, filtrar
Spark pode otimizar estas analises através de planos de execução

**Lazy Evalution**

O processamento de  transforamção de fato só ocorre quando há uma ação: Lazy Evalution

#####  Tipos de Dados  

> ByteType
> ShortType
> IntegerType
> LongType
> FloatType
> DoubleType
> DecimalType
> StringType
> BinaryType
> BooleanType
> TimestampType
> DateType
> ArrayType
> MapType
> StructType
> StructField

##### Schema

Você pode deixar o Spark inferir a partir de parte dos dados ou pode definir o schema.
Definindo você tem vantagens de ter o tipo correto e sem overhead.




#### Data Frame - parte 2

Criação de um data frame, simples e sem definição de schema.

<code> pyspark </code>

from pyspark import SparkSession
df1 = spark.createDataFrame([("Pedro",10),("Maria",30),("José",40)])
df1.show()

    +-----+---+
    |   _1| _2|
    +-----+---+
    |Pedro| 10|
    |Maria| 30|
    | José| 40|
    +-----+---+


schema = "Id  INT, Nome String"
dados = [[1,"Pedro"],[2,"Maria"]]
df2 = spark.createDataFrame(dados, schema)
df2.show()

    +---+-----+
    | Id| Nome|
    +---+-----+
    |  1|Pedro|
    |  2|Maria|
    +---+-----+

from pyspark.sql.functions import sum
schema2  =  "Produtos STRING, Vendas  INT"
vendas = [["Caneta",10], ["Lápis", 20],  ["Caneta",  40]]
df3  = spark.createDataFrame(vendas, schema2)
df3.show()

    +--------+------+
    |Produtos|Vendas|
    +--------+------+
    |  Caneta|    10|
    |   Lápis|    20|
    |  Caneta|    40|
    +--------+------+

agrupado = df3.groupBy("Produtos").agg(sum("Vendas"))
agrupado.show()

    +--------+-----------+
    |Produtos|sum(Vendas)|
    +--------+-----------+
    |  Caneta|         50|
    |   Lápis|         20|
    +--------+-----------+


df3.groupBy("Produtos").agg(sum("Vendas")).show()

    +--------+-----------+
    |Produtos|sum(Vendas)|
    +--------+-----------+
    |  Caneta|         50|
    |   Lápis|         20|
    +--------+-----------+

df3.select("Produtos").show()

    +--------+
    |Produtos|
    +--------+
    |  Caneta|
    |   Lápis|
    |  Caneta|
    +--------+

--função de expressão, criar nova coluna
from pyspark.sql.functions import expr  
df3.select("Produtos","Vendas",expr("Vendas *  0.2")).show()

    +--------+------+--------------+
    |Produtos|Vendas|(Vendas * 0.2)|
    +--------+------+--------------+
    |  Caneta|    10|           2.0|
    |   Lápis|    20|           4.0|
    |  Caneta|    40|           8.0|
    +--------+------+--------------+


#### Data Frame - parte 3

df3.schema
StructType([StructField('Produtos', StringType(), True), StructField('Vendas', IntegerType(), True)])

df3.columns
['Produtos', 'Vendas']

##### Ingestão de arquivo csv

from pyspark.sql.types import *
arschema = 'id INT, nome  STRING, status STRING, cidade STRING, vendas INT, data STRING'

despachantes = spark.read.csv("/home/luciana/download/despachantes.csv", header = False, schema = arschema)
despachantes.show()

    +---+-------------------+------+-------------+------+----------+
    | id|               nome|status|       cidade|vendas|      data|
    +---+-------------------+------+-------------+------+----------+
    |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
    |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
    |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
    |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
    |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
    |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
    |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
    |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
    |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
    | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
    +---+-------------------+------+-------------+------+----------+


desp_autoschema =  spark.read.load("/home/luciana/download/despachantes.csv", header = False, format="csv", sep=",", inferSchema=True)
desp_autoschema.show()

    +---+-------------------+-----+-------------+---+-------------------+
    |_c0|                _c1|  _c2|          _c3|_c4|                _c5|
    +---+-------------------+-----+-------------+---+-------------------+
    |  1|   Carminda Pestana|Ativo|  Santa Maria| 23|2020-08-11 00:00:00|
    |  2|    Deolinda Vilela|Ativo|Novo Hamburgo| 34|2020-03-05 00:00:00|
    |  3|   Emídio Dornelles|Ativo| Porto Alegre| 34|2020-02-05 00:00:00|
    |  4|Felisbela Dornelles|Ativo| Porto Alegre| 36|2020-02-05 00:00:00|
    |  5|     Graça Ornellas|Ativo| Porto Alegre| 12|2020-02-05 00:00:00|
    |  6|   Matilde Rebouças|Ativo| Porto Alegre| 22|2019-01-05 00:00:00|
    |  7|    Noêmia   Orriça|Ativo|  Santa Maria| 45|2019-10-05 00:00:00|
    |  8|      Roque Vásquez|Ativo| Porto Alegre| 65|2020-03-05 00:00:00|
    |  9|      Uriel Queiroz|Ativo| Porto Alegre| 54|2018-05-05 00:00:00|
    | 10|   Viviana Sequeira|Ativo| Porto Alegre|  0|2020-09-05 00:00:00|
    +---+-------------------+-----+-------------+---+-------------------+

despachantes.schema
StructType([StructField('id', IntegerType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('cidade', StringType(), True), StructField('vendas', IntegerType(), True), StructField('data', StringType(), True)])

desp_autoschema.schema
StructType([StructField('_c0', IntegerType(), True), StructField('_c1', StringType(), True), StructField('_c2', StringType(), True), StructField('_c3', StringType(), True), StructField('_c4', IntegerType(), True), StructField('_c5', TimestampType(), True)])





#### Data Frame - parte 4

#####  Filtros

from pyspark.sql  import functions as Func
despachantes.select("id","nome","vendas").where(Func.col("vendas") > 20).show()

    +---+-------------------+------+
    | id|               nome|vendas|
    +---+-------------------+------+
    |  1|   Carminda Pestana|    23|
    |  2|    Deolinda Vilela|    34|
    |  3|   Emídio Dornelles|    34|
    |  4|Felisbela Dornelles|    36|
    |  6|   Matilde Rebouças|    22|
    |  7|    Noêmia   Orriça|    45|
    |  8|      Roque Vásquez|    65|
    |  9|      Uriel Queiroz|    54|
    +---+-------------------+------+

Operadores  lógicos  do spark:
&(e), |(ou), ~(não)

despachantes.select("id","nome","vendas").where((Func.col("vendas") > 20)  & (Func.col("vendas") <  40)).show()

    +---+-------------------+------+
    | id|               nome|vendas|
    +---+-------------------+------+
    |  1|   Carminda Pestana|    23|
    |  2|    Deolinda Vilela|    34|
    |  3|   Emídio Dornelles|    34|
    |  4|Felisbela Dornelles|    36|
    |  6|   Matilde Rebouças|    22|
    +---+-------------------+------+

<code>Como faço para alterar ou deletar um schema?</code>
Como um data frame no spark é imutável, iremos criar um novo dataframe.

#####  Editando schema no spark

**Alterando nome de coluna** withColumnRenamed

novodf = despachantes. withColumnRenamed("nome", "nomes")
novodf.columns
['id', 'nomes', 'status', 'cidade', 'vendas', 'data']

**Mudando o data type de coluna** withColumn

criando nova coluna, depois pode dropar a coluna data original

from pyspark.sql.functions import *
despachantes2 =  despachantes.withColumn("data2", to_timestamp(Func.col("data"), "yyyy-MM-dd"))
despachantes2.schema
StructType([StructField('id', IntegerType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('cidade', StringType(), True), StructField('vendas', IntegerType(), True), StructField('data', StringType(), True), StructField('data2', TimestampType(), True)])

despachantes2.show()

    +---+-------------------+------+-------------+------+----------+-------------------+
    | id|               nome|status|       cidade|vendas|      data|              data2|
    +---+-------------------+------+-------------+------+----------+-------------------+
    |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|2020-08-11 00:00:00|
    |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|2020-03-05 00:00:00|
    |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|2020-02-05 00:00:00|
    |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|2020-02-05 00:00:00|
    |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|2020-02-05 00:00:00|
    |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|2019-01-05 00:00:00|
    |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|2019-10-05 00:00:00|
    |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|2020-03-05 00:00:00|
    |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|2018-05-05 00:00:00|
    | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|2020-09-05 00:00:00|
    +---+-------------------+------+-------------+------+----------+-------------------+

**usando funções de date em coluna string**

despachantes2.select(year("data")).show()

    +----------+
    |year(data)|
    +----------+
    |      2020|
    |      2020|
    |      2020|
    |      2020|
    |      2020|
    |      2019|
    |      2019|
    |      2020|
    |      2018|
    |      2020|
    +----------+

despachantes2.select(year("data")).distinct().show()

    +----------+
    |year(data)|
    +----------+
    |      2018|
    |      2019|
    |      2020|
    +----------+

despachantes2.select("nome",year("data")).orderBy("nome").show()

    +-------------------+----------+
    |               nome|year(data)|
    +-------------------+----------+
    |   Carminda Pestana|      2020|
    |    Deolinda Vilela|      2020|
    |   Emídio Dornelles|      2020|
    |Felisbela Dornelles|      2020|
    |     Graça Ornellas|      2020|
    |   Matilde Rebouças|      2019|
    |    Noêmia   Orriça|      2019|
    |      Roque Vásquez|      2020|
    |      Uriel Queiroz|      2018|
    |   Viviana Sequeira|      2020|
    +-------------------+----------+

despachantes2.select("data").groupBy(year("data")).count().show()

    +----------+-----+
    |year(data)|count|
    +----------+-----+
    |      2018|    1|
    |      2019|    2|
    |      2020|    7|
    +----------+-----+

despachantes2.select(Func.sum("vendas")).show()

    +-----------+
    |sum(vendas)|
    +-----------+
    |        325|
    +-----------+

despachantes2.select("data",Func.sum("vendas")).where(year("data") = 2018).show()
<code>Como transformar mais tudo junto?</code>


despachantes.show()


#### Principais Ações e Transformações

<br/>

* despachantes.show()


        +---+-------------------+------+-------------+------+----------+
        | id|               nome|status|       cidade|vendas|      data|
        +---+-------------------+------+-------------+------+----------+
        |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
        |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
        |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
        |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
        |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
        |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
        |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
        |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
        |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
        | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
        +---+-------------------+------+-------------+------+----------+


<br/>

* despachantes.take(1) - em lista
  
        [Row(id=1, nome='Carminda Pestana', status='Ativo', cidade='Santa Maria', vendas=23, data='2020-08-11')]


<br/>

* despachantes.collect() - ação herdada do RDD
  
        [Row(id=1, nome='Carminda Pestana', status='Ativo', cidade='Santa Maria', vendas=23, data='2020-08-11'), Row(id=2, nome='Deolinda Vilela', status='Ativo', cidade='Novo Hamburgo', vendas=34, data='2020-03-05'), Row(id=3, nome='Emídio Dornelles', status='Ativo', cidade='Porto Alegre', vendas=34, data='2020-02-05'), Row(id=4, nome='Felisbela Dornelles', status='Ativo', cidade='Porto Alegre', vendas=36, data='2020-02-05'), Row(id=5, nome='Graça Ornellas', status='Ativo', cidade='Porto Alegre', vendas=12, data='2020-02-05'), Row(id=6, nome='Matilde Rebouças', status='Ativo', cidade='Porto Alegre', vendas=22, data='2019-01-05'), Row(id=7, nome='Noêmia   Orriça', status='Ativo', cidade='Santa Maria', vendas=45, data='2019-10-05'), Row(id=8, nome='Roque Vásquez', status='Ativo', cidade='Porto Alegre', vendas=65, data='2020-03-05'), Row(id=9, nome='Uriel Queiroz', status='Ativo', cidade='Porto Alegre', vendas=54, data='2018-05-05'), Row(id=10, nome='Viviana Sequeira', status='Ativo', cidade='Porto Alegre', vendas=0, data='2020-09-05')]


<br/>

* despachantes.count() 
  10


<br/>

* despachantes.orderBy("vendas").show()
 

        +---+-------------------+------+-------------+------+----------+
        | id|               nome|status|       cidade|vendas|      data|
        +---+-------------------+------+-------------+------+----------+
        | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
        |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
        |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
        |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
        |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
        |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
        |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
        |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
        |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
        |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
        +---+-------------------+------+-------------+------+----------+

<br/>

* despachantes.orderBy(Func.col("vendas").desc()).show()


        +---+-------------------+------+-------------+------+----------+
        | id|               nome|status|       cidade|vendas|      data|
        +---+-------------------+------+-------------+------+----------+
        |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
        |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
        |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
        |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
        |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
        |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
        |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
        |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
        |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
        | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
        +---+-------------------+------+-------------+------+----------+

<br/>

* despachantes.orderBy(Func.col("cidade").desc(), Func.col("vendas").desc() ).show()
  
        +---+-------------------+------+-------------+------+----------+
        | id|               nome|status|       cidade|vendas|      data|
        +---+-------------------+------+-------------+------+----------+
        |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
        |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
        |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
        |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
        |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
        |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
        |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
        |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
        | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
        |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
        +---+-------------------+------+-------------+------+----------+


<br/>

* despachantes.groupBy("cidade").agg(sum("vendas")).show()

        +-------------+-----------+
        |       cidade|sum(vendas)|
        +-------------+-----------+
        |  Santa Maria|         68|
        |Novo Hamburgo|         34|
        | Porto Alegre|        223|
        +-------------+-----------+

<br/>

* despachantes.groupBy("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()


        +-------------+-----------+
        |       cidade|sum(vendas)|
        +-------------+-----------+
        | Porto Alegre|        223|
        |  Santa Maria|         68|
        |Novo Hamburgo|         34|
        +-------------+-----------+


<br/>

* despachantes.filter(Func.col("nome") == "Deolinda Vilela").show()

        +---+---------------+------+-------------+------+----------+
        | id|           nome|status|       cidade|vendas|      data|
        +---+---------------+------+-------------+------+----------+
        |  2|Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
        +---+---------------+------+-------------+------+----------+

<br/>

#### Exportando dados

<br/>

despachantes.write.format("parquet").save("/home/luciana/dfimportparquet")

despachantes.write.format("csv").save("/home/luciana/dfimportcsv")

despachantes.write.format("json").save("/home/luciana/dfimportjson")

despachantes.write.format("orc").save("/home/luciana/dfimportorc")

<br/>

#### Importando dados

<br/>

 mv part-00000-41f7ccad-c00b-47cc-9542-5c75d2580b64-c000.csv despachanes.csv
para todos (json, orc e parquet)

*importando*

par  = spark.read.format("parquet").load("/home/luciana/dfimportparquet/despachantes.parquet")
par.show()

        +---+-------------------+------+-------------+------+----------+
        | id|               nome|status|       cidade|vendas|      data|
        +---+-------------------+------+-------------+------+----------+
        |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
        |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
        |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
        |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
        |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
        |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
        |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
        |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
        |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
        | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
        +---+-------------------+------+-------------+------+----------+

par.schema
StructType([StructField('id', IntegerType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('cidade', StringType(), True), StructField('vendas', IntegerType(), True), StructField('data', StringType(), True)])

<br/>

js = spark.read.format("json").load("/home/luciana/dfimportjson/despachantes.json")
js.show()

    +-------------+----------+---+-------------------+------+------+
    |       cidade|      data| id|               nome|status|vendas|
    +-------------+----------+---+-------------------+------+------+
    |  Santa Maria|2020-08-11|  1|   Carminda Pestana| Ativo|    23|
    |Novo Hamburgo|2020-03-05|  2|    Deolinda Vilela| Ativo|    34|
    | Porto Alegre|2020-02-05|  3|   Emídio Dornelles| Ativo|    34|
    | Porto Alegre|2020-02-05|  4|Felisbela Dornelles| Ativo|    36|
    | Porto Alegre|2020-02-05|  5|     Graça Ornellas| Ativo|    12|
    | Porto Alegre|2019-01-05|  6|   Matilde Rebouças| Ativo|    22|
    |  Santa Maria|2019-10-05|  7|    Noêmia   Orriça| Ativo|    45|
    | Porto Alegre|2020-03-05|  8|      Roque Vásquez| Ativo|    65|
    | Porto Alegre|2018-05-05|  9|      Uriel Queiroz| Ativo|    54|
    | Porto Alegre|2020-09-05| 10|   Viviana Sequeira| Ativo|     0|
    +-------------+----------+---+-------------------+------+------+

js.schema
StructType([StructField('cidade', StringType(), True), StructField('data', StringType(), True), StructField('id', LongType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('vendas', LongType(), True)])

<br/>

orc = spark.read.format("orc").load("/home/luciana/dfimportorc/despachantes.orc")
orc.show()

    +---+-------------------+------+-------------+------+----------+
    | id|               nome|status|       cidade|vendas|      data|
    +---+-------------------+------+-------------+------+----------+
    |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
    |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
    |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
    |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
    |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
    |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
    |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
    |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
    |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
    | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
    +---+-------------------+------+-------------+------+----------+


orc.schema
StructType([StructField('id', IntegerType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('cidade', StringType(), True), StructField('vendas', IntegerType(), True), StructField('data', StringType(), True)])

<br/>

cs = spark.read.format("csv").load("/home/luciana/dfimportcsv/despachanes.csv")
cs.show()

    +---+-------------------+-----+-------------+---+----------+
    |_c0|                _c1|  _c2|          _c3|_c4|       _c5|
    +---+-------------------+-----+-------------+---+----------+
    |  1|   Carminda Pestana|Ativo|  Santa Maria| 23|2020-08-11|
    |  2|    Deolinda Vilela|Ativo|Novo Hamburgo| 34|2020-03-05|
    |  3|   Emídio Dornelles|Ativo| Porto Alegre| 34|2020-02-05|
    |  4|Felisbela Dornelles|Ativo| Porto Alegre| 36|2020-02-05|
    |  5|     Graça Ornellas|Ativo| Porto Alegre| 12|2020-02-05|
    |  6|   Matilde Rebouças|Ativo| Porto Alegre| 22|2019-01-05|
    |  7|    Noêmia   Orriça|Ativo|  Santa Maria| 45|2019-10-05|
    |  8|      Roque Vásquez|Ativo| Porto Alegre| 65|2020-03-05|
    |  9|      Uriel Queiroz|Ativo| Porto Alegre| 54|2018-05-05|
    | 10|   Viviana Sequeira|Ativo| Porto Alegre|  0|2020-09-05|
    +---+-------------------+-----+-------------+---+----------+

cs.schema
StructType([StructField('_c0', StringType(), True), StructField('_c1', StringType(), True), StructField('_c2', StringType(), True), StructField('_c3', StringType(), True), StructField('_c4', StringType(), True), StructField('_c5', StringType(), True)])

<br/>

arschema - ja criado antes
'id INT, nome  STRING, status STRING, cidade STRING, vendas INT, data STRING'


cs2 = spark.read.format("csv").load("/home/luciana/dfimportcsv/despachanes.csv", schema=arschema)
cs2.show()

    +---+-------------------+------+-------------+------+----------+
    | id|               nome|status|       cidade|vendas|      data|
    +---+-------------------+------+-------------+------+----------+
    |  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
    |  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
    |  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
    |  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
    |  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
    |  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
    |  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
    |  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
    |  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
    | 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
    +---+-------------------+------+-------------+------+----------+

cs2.schema

    StructType([StructField('id', IntegerType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('cidade', StringType(), True), StructField('vendas', IntegerType(), True), StructField('data', StringType(), True)])

<br/>

#### Atividades: Faça você mesmo

<br/>

Localizada na pasta:  atividades-praticas\secao03-faca-voce-mesmo.md