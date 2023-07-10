from pyspark.sql import SparkSession
from pyspark.sql.types import *


def execute():
    spark = SparkSession.builder.getOrCreate()
    # spark.sql("create database desp")  # Esse comando só pode ser utilizado uma unica vez
    spark.sql("show databases").show()
    spark.sql("use desp")

    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes = spark.read.csv(
        "/home/leonardo/Documentos/curso_pyspark/despachantes.csv",
        header=False,
        schema=arqschema
    )

    # despachantes.write.saveAsTable("Despachantes")

    spark.sql("select * from despachantes").show()
