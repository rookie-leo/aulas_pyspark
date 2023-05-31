from data_frame.data_frame import executar, transformacao
from data_frame.exportar_dados import exportar
from data_frame.importar_dados import importar

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # transformacao("/home/leonardo/Documentos/curso_pyspark/csv/despachantes.csv")
    # exportar("JSON", "/home/leonardo/Documentos/curso_pyspark/json")
    importar("parquet", "/home/leonardo/Documentos/curso_pyspark/parquet/despachantes.parquet")
