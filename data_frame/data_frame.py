from pyspark.shell import spark
from pyspark.sql.functions import *


def executar(path: str) -> None:
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes = spark.read.csv(f"{path}", header=False, schema=arqschema)
    despachantes.show()

    despachantes.select("id", "nome", "vendas").where((col("vendas") > 20) & (col("vendas") < 40)).show()

    despachantes2 = despachantes.withColumn("data2", to_timestamp(col("data"), "yyyy-MM-dd"))

    despachantes2.show()

    despachantes2.select(year("data")).show()

    despachantes2.select(year("data")).distinct().show()

    despachantes2.select("nome", year("data")).orderBy("nome").show()

    despachantes2.select("data").groupby(year("data")).count().show()

    despachantes2.select(sum("vendas")).show()
