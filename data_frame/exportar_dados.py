from pyspark.sql import SparkSession

"""
    :parameter format: recebe formatos aceitos pelo spark (JSON, CSV, parquet, orc)
"""

def exportar(format: str, path: str) -> None:
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    spark = SparkSession.builder.getOrCreate()
    despachantes = spark.createDataFrame([], arqschema)

    despachantes.write.format(format).save(path)
