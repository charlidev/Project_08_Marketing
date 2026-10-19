import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

# Ruta apuntando a la carpeta 'landing' dentro de tu contenedor 'marketing' en ADLS
# Nota: Validamos el nombre del storage account basándonos en tu captura. 
# Si el nombre completo no es exactamente 'datalakedino14_1792423117138', ajústalo.
ruta_adls = "abfss://marketing@datalakedino14_1792423117138.dfs.core.windows.net/landing/"

@dlt.table(
    name="bronze_crm_marketing",
    comment="Capa Bronze: Datos crudos ingestados desde la zona landing en ADLS usando Autoloader.",
    table_properties={"quality": "bronze"}
)
def bronze_crm_marketing():
    """
    Lee archivos CSV de forma incremental desde la carpeta 'landing' en ADLS usando Databricks Autoloader.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true") # Infiere el esquema automáticamente
        .option("header", "true")
        .load(ruta_adls)
        # Agregamos metadatos de auditoría (buenas prácticas de Data Engineering)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )