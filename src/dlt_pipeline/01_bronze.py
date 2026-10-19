import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp
from pyspark.sql import SparkSession

ruta_adls = "abfss://marketing@datalakedino14.dfs.core.windows.net/landing/"

@dlt.table(
    name="bronze_crm_marketing",
    comment="Capa Bronze: Datos crudos ingestados desde la zona landing en ADLS usando Autoloader.",
    table_properties={"quality": "bronze"}
)
def bronze_crm_marketing():
    spark = SparkSession.builder.getOrCreate()
    
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true") 
        .option("header", "true")
        .load(ruta_adls)
        .withColumn("ingestion_timestamp", current_timestamp())
        # Corrección: Usamos _metadata.file_path en lugar de input_file_name()
        .withColumn("source_file", F.col("_metadata.file_path"))
    )