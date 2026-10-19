import dlt
import pyspark.sql.functions as F

# Creamos una vista temporal en DLT para generar el ID único y evitar recalcularlo
@dlt.view(name="silver_with_id")
def silver_with_id():
    return (
        dlt.read("silver_crm_marketing")
        # Generamos una llave subrogada (Customer_ID) usando el hash de toda la fila
        # para garantizar que el mismo cliente tenga siempre el mismo ID
        .withColumn("Customer_ID", F.md5(F.to_json(F.struct("*"))))
    )

@dlt.table(
    name="dim_clientes",
    comment="Dimensión: Perfil demográfico y estático del cliente."
)
def dim_clientes():
    return (
        dlt.read("silver_with_id")
        .withColumn("Total_Dependents", F.col("Kidhome") + F.col("Teenhome"))
        .select(
            "Customer_ID", "Age", "Income", "Total_Dependents", 
            "marital_Divorced", "marital_Married", "marital_Single", "marital_Together", "marital_Widow",
            "education_2n Cycle", "education_Basic", "education_Graduation", "education_Master", "education_PhD",
            "Customer_Days"
        )
    )

@dlt.table(
    name="fact_compras",
    comment="Tabla de Hechos: Transacciones, gastos por categoría y canales de compra."
)
def fact_compras():
    return (
        dlt.read("silver_with_id")
        .select(
            "Customer_ID", "Recency", "MntTotal", "MntRegularProds", 
            "MntWines", "MntFruits", "MntMeatProducts", "MntFishProducts", "MntSweetProducts", "MntGoldProds",
            "NumDealsPurchases", "NumWebPurchases", "NumCatalogPurchases", "NumStorePurchases", "NumWebVisitsMonth"
        )
    )

@dlt.table(
    name="fact_campanias",
    comment="Tabla de Hechos: Rendimiento y respuestas a las campañas de marketing."
)
def fact_campanias():
    return (
        dlt.read("silver_with_id")
        .select(
            "Customer_ID", 
            "AcceptedCmp1", "AcceptedCmp2", "AcceptedCmp3", "AcceptedCmp4", "AcceptedCmp5", 
            "AcceptedCmpOverall", "Response", "Complain"
        )
    )