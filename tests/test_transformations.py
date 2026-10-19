import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Esta función levanta una sesión de Spark local solo para las pruebas
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("UnitTests") \
        .master("local[*]") \
        .getOrCreate()

def test_silver_customer_id_generation(spark):
    """Prueba que el Customer_ID se genere correctamente con un Hash MD5"""
    # 1. Simulamos datos crudos que vienen de la capa Bronze
    data = [{"Year_Birth": 1990, "Education": "Graduation"}]
    df = spark.createDataFrame(data)
    
    # 2. Aplicamos la misma lógica que usamos en tu capa Silver
    df_silver = df.withColumn("Customer_ID", F.md5(F.to_json(F.struct("*"))))
    
    # 3. Comprobamos (assert) que la columna exista y tenga un valor
    assert "Customer_ID" in df_silver.columns
    assert df_silver.collect()[0]["Customer_ID"] is not None

def test_dim_clientes_dependents(spark):
    """Prueba que la suma de niños y adolescentes (Total_Dependents) sea correcta en Gold"""
    # 1. Simulamos datos limpios de la capa Silver
    data = [{"Customer_ID": "123", "Kidhome": 1, "Teenhome": 2}]
    df = spark.createDataFrame(data)
    
    # 2. Aplicamos la misma lógica que usamos en tu capa Gold
    df_gold = df.withColumn("Total_Dependents", F.col("Kidhome") + F.col("Teenhome"))
    
    # 3. Comprobamos que 1 niño + 2 adolescentes sea exactamente igual a 3
    resultado = df_gold.collect()[0]["Total_Dependents"]
    assert resultado == 3