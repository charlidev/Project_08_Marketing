# CRM Marketing Data Pipeline 🚀

End-to-end Data Engineering project demonstrating enterprise-grade practices, including a declarative Medallion architecture, CI/CD pipelines, unit testing, and environment promotion using Databricks Asset Bundles (DABs).

## 📌 Arquitectura y Tecnologías
* **Procesamiento:** Azure Databricks, Serverless Delta Live Tables (DLT), PySpark.
* **Gobierno de Datos:** Unity Catalog (External Locations, Storage Credentials).
* **Orquestación y CI/CD:** GitHub Actions, Databricks CLI.
* **Pruebas y Calidad:** `pytest`, `pytest-cov`, DLT Expectations.
* **Visualización:** Power BI (Import Mode via PAT).

## ⚙️ Características del Pipeline

### 1. Arquitectura Medallón (DLT)
Flujo de datos automatizado y declarativo:
* **Bronze:** Ingesta incremental de datos crudos desde ADLS utilizando **Auto Loader** (`cloudFiles`), capturando metadatos de linaje (`_metadata.file_path`).
* **Silver:** Limpieza de datos, generación de llaves subrogadas (MD5 hashes) y aplicación de reglas de calidad mediante *DLT Expectations*.
* **Gold:** Modelado dimensional (Star Schema) diseñado para consumo analítico rápido, separando `dim_clientes`, `fact_compras` y `fact_campanias`.

### 2. CI/CD y Calidad de Código
Implementación de un pipeline multi-job en **GitHub Actions**:
* **Job 1 (Unit Tests):** Levanta una sesión local de Spark para ejecutar pruebas unitarias con `pytest`, validando la lógica de las transformaciones matemáticas y de negocio. Genera un reporte de cobertura (`pytest-cov`).
* **Job 2 (Deploy):** Solo si las pruebas son exitosas, utiliza Databricks Asset Bundles para compilar y desplegar el código en el Data Lake.

### 3. Promoción de Ambientes (dev, test, prod)
Gestión de ciclo de vida del software integrado con Git. El ruteo dinámico en GitHub Actions despliega la infraestructura en esquemas separados dentro de Unity Catalog (`marketing_schema_dev`, `test`, `prod`) dependiendo de la rama que dispare el evento (`dev`, `test`, `main`).

---
*Desarrollado como portafolio técnico de Data Engineering.*