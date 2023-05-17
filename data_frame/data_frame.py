from pyspark.shell import spark
from pyspark.sql import functions as Func


def executar(path: str) -> None:
    arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
    despachantes = spark.read.csv(f"{path}", header=False, schema=arqschema)
    despachantes.show()

    # despachantes.select("id", "nome", "vendas").where(Func.col("vendas"))
