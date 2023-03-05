## Seção 4: Spark SQL

### Spark SQL


Utiliza o metastore do Hive, porém não é preciso ter o Hive instalado para usar o Spark.


####  Tabela

- Persistente
- Objeto tabular que reside em um banco de dados
- Pode ser gerenciado e consultado utilizando SQL
- Totalmente interoperável com DataFrame
- Ex:  Você pode transformar um DataFrame importante (parquet, json, orc, csv) em tabela


**Gerenciadas: Spark  gerencia dados e metadados**

Armazenadas no warehouse do spark. Se excluirmos, tudo é apagado  (dados e metadados)

**Não Gerenciadas (External): Spark  apenas gerencia metadados**

Informamos onde a tabela está  (arquivo, por exemplo orc)
Se excluirmos, Spark só exclui os metadados, dados permanecem onde estavam


####  Views

-  Mesmo conceito de banco de dados relacionais
-  São um "alias" para uma tabela (por exemplo, vendas_rs pode mostrar vendas do estado já com o filtro aplicado)
-  Não contém dados


**Globais**:  visíveis em todas as sessões
**Sessão**: visíveis apenas na própria sessão


### Banco de dados e Tabelas

$ pyspark 

    from pyspark.sql import SparkSession
    from pyspark.sql.types import *


    spark.sql("show databases").show()

    spark.sql("create database desp")

        +---------+
        |namespace|
        +---------+
        |  default|
        |     desp|
        +---------+


    spark.sql("use desp").show()

Criar Dataframe

    arschema = 'id INT, nome  STRING, status STRING, cidade STRING, vendas INT, data STRING'

    despachantes = spark.read.csv("/home/luciana/download/despachantes.csv", header = False, schema = arschema)

    despachantes.show()

Dataframe criado - Criar tabela

    despachantes.write.saveAsTable("Despachantes")

    spark.sql("show tables").show()

    spark.sql("select * from despachantes").show()


spark.sql("use default").show()

Recriar a tabela

    despachantes.write.mode("overwrite").saveAsTable("Despachantes")
    spark.sql("select * from despachantes").show()

