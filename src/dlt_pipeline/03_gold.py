import dlt
import pyspark.sql.functions as F

@dlt.view(name="silver_with_id")
def silver_with_id():
    return (
        dlt.read("silver_crm_marketing")
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
            F.col("Customer_ID"), 
            F.col("Age"), 
            F.col("Income"), 
            F.col("Total_Dependents"), 
            F.col("marital_Divorced"), 
            F.col("marital_Married"), 
            F.col("marital_Single"), 
            F.col("marital_Together"), 
            F.col("marital_Widow"),
            # AQUÍ REPARAMOS EL ERROR DEL ESPACIO:
            F.col("education_2n Cycle").alias("education_2n_Cycle"), 
            F.col("education_Basic"), 
            F.col("education_Graduation"), 
            F.col("education_Master"), 
            F.col("education_PhD"),
            F.col("Customer_Days")
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