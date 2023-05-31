from pyspark.shell import spark

def importar(format: str, path: str) -> None:
    file = spark.read.format(format).load(path)

    file.show()

