import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="silver_crm_marketing",
    comment="Capa Silver: Datos de marketing limpios, sin duplicados y con reglas de Data Quality.",
    table_properties={"quality": "silver"}
)
# Regla 1: Descartar filas donde el ingreso sea nulo o negativo
@dlt.expect_or_drop("ingreso_valido", "Income IS NOT NULL AND Income >= 0")

# Regla 2: La edad del cliente debe ser lógica (mayor de edad y menor de 100 años)
@dlt.expect_or_drop("edad_logica", "Age >= 18 AND Age <= 100")

# Regla 3: No pueden existir gastos totales negativos (evitar devoluciones mal registradas)
@dlt.expect_or_drop("gasto_positivo", "MntTotal >= 0")

def silver_crm_marketing():
    """
    Lee los datos crudos de la capa Bronze en streaming, 
    elimina duplicados exactos y aplica las expectativas de calidad.
    """
    # Usamos read_stream porque Bronze fue creado con Autoloader (streaming)
    df_bronze = dlt.read_stream("bronze_crm_marketing")
    
    # Limpieza básica: Eliminamos duplicados exactos en toda la fila
    df_clean = df_bronze.dropDuplicates()
    
    return df_clean