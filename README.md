# 🚀 End-to-End Big Data Lakehouse, PySpark Streaming & AI Auditor (`bigdata-ai-stack`)

**Author:** **José Luis Herrera Reyna** — *Data & Analytics Engineer (AI-First)*  
📍 Durango, México (CST Timezone — US & LATAM Remote) | 📧 [luisblueberry@gmail.com](mailto:luisblueberry@gmail.com) | 📱 +52 618 332 9565 | 💻 [github.com/blueberrymx](https://github.com/blueberrymx)

---

## 🌎 Executive Summary (English)

Production-grade **Big Data Lakehouse & Real-Time Streaming** portfolio project built with **Medallion Architecture (Bronze / Silver / Gold)**, **Star Schema dimensional modeling**, **Apache Parquet (ZSTD)**, **DuckDB OLAP SQL**, and **PySpark Structured Streaming**, validated by an automated **10-Test SQL Data Quality & Financial Reconciliation Suite**.

### Key Engineering Results
* **Storage & Speed Optimization:** Processed **52,022 raw transactions**, purging **4,549 corrupt/duplicate rows** via Primary Key Window Functions (`QUALIFY ROW_NUMBER() OVER (PARTITION BY id_venta)`) and reducing storage footprint by **84.0%** (`1,784.3 KB` CSV $\rightarrow$ `286.3 KB` Parquet ZSTD) in **0.48 seconds**.
* **Double Star-Schema JOIN (Gold Layer):** Joined `fact_ventas` $\bowtie$ `dim_clientes` (`id_cliente`) $\bowtie$ `dim_productos` (`id_producto`) to deliver the executive corporate segmentation report (`kpi_ventas_enterprise_mexico.parquet`), reconciling **\$3,612,580.00 USD** across `'Enterprise'` accounts in `'Mexico'` by product category.
* **PySpark Structured Streaming & Dead-Letter Quarantine:** Implemented real-time micro-batch ingestion with explicit `StructType` schemas, `withWatermark`, and `F.broadcast()` hash joins, isolating **78 live anomalies** (`28` negative amounts, `23` null customer keys, `15` orphan product IDs `id=999`, and `12` inter-batch duplicates) in `quarantine_errores_stream.parquet` with **0% silent data loss**.
* **Automated Observability (`10/10` Tests Passed):** Certified PK uniqueness, referential integrity, and cent-exact financial balance across Batch and Streaming layers.

---

## 🏗️ Project Architecture / Arquitectura del Proyecto

```text
bigdata-ai-stack/
├── data/
│   ├── bronze/   🥉 Raw CSV data (52,022 batch rows) + Live Streaming micro-batches (stream_inbox/)
│   ├── silver/   🥈 Clean ZSTD Parquet tables (Star Schema) + Dead-Letter Quarantine table
│   └── gold/     🥇 Executive KPI Parquet tables (By Country, Product, Enterprise Mexico Batch & RT)
├── docs/
│   └── CV_Jose_Luis_Herrera_Data_Engineer.html  # Bilingual (EN/ES) ATS-Friendly Printable Resume
├── src/
│   ├── generar_datos_bronze.py        # Step 1: Raw data ingestion simulator with realistic anomalies
│   ├── pipeline_medallion.py          # Step 2: Medallion ELT (Window Function Deduplication + Star Schema JOINs)
│   ├── streaming_pyspark_medallion.py # Step 3: PySpark Structured Streaming (Watermarking + Broadcast + Quarantine)
│   └── agente_auditor_sql.py          # Step 4: Automated 10-Test Data Quality & Financial Reconciliation Suite
└── README.md
```

---

## 📊 Gold Layer Benchmark: `'Enterprise'` Segment in `'Mexico'` by Category

| Category (`categoria`) | Operations | Unique Accounts | Units Sold | Total Revenue (USD) | Market Share |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Tecnología** | 980 | 30 | 2,936 | **\$2,439,450.00** | **67.53%** |
| **Muebles** | 495 | 30 | 1,486 | **\$520,100.00** | **14.40%** |
| **Audio** | 490 | 30 | 1,499 | **\$374,750.00** | **10.37%** |
| **Accesorios** | 951 | 30 | 2,817 | **\$278,280.00** | **7.70%** |
| **TOTAL MÉXICO ENTERPRISE** | **2,916** | **30** | **8,738** | **\$3,612,580.00** | **100.00%** |

---

## ▶️ Quickstart / Cómo Ejecutar el Pipeline Completo

```powershell
# 1. Generate 52,022 raw Bronze records
python src/generar_datos_bronze.py

# 2. Run Medallion ELT Pipeline (Bronze -> Silver Parquet -> 3 Gold Star Schema KPI Tables)
python src/pipeline_medallion.py

# 3. Run Real-Time Streaming Pipeline (PySpark Structured Streaming + Dead-Letter Quarantine)
python src/streaming_pyspark_medallion.py

# 4. Run Automated 10-Test Data Quality & Reconciliation Auditor
python src/agente_auditor_sql.py
```
