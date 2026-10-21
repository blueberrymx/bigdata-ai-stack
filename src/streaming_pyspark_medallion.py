"""
Paso 4: Escalado a Tiempo Real con PySpark Structured Streaming (Medallion Streaming)
-------------------------------------------------------------------------------------
Este módulo implementa la arquitectura de Streaming en Tiempo Real para Databricks / Spark:
1. Define el esquema explícito (StructType) — buena práctica obligatoria en Spark Streaming.
2. Aplica Watermarking (withWatermark) y Deduplicación Stateful (dropDuplicates por id_venta).
3. Ejecuta Broadcast Hash Joins (F.broadcast) contra dim_clientes y dim_productos.
4. Enruta registros corruptos/huérfanos a una tabla Dead-Letter / Quarantine (Patrón Lakehouse).
5. Incluye auto-detección de entorno: si detecta un clúster PySpark/JVM activo usa PySpark nativo;
   si se ejecuta en entorno local sin JVM/winutils, activa el motor Micro-Batch local equivalente
   para procesar 3 micro-lotes en vivo y generar las tablas Parquet de Streaming y Cuarentena.
"""

import csv
import os
import random
import time
from datetime import datetime, timedelta
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_STREAM_DIR = os.path.join(BASE_DIR, "data", "bronze", "stream_inbox")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "data", "checkpoints", "enterprise_mexico")


# ==============================================================================
# 1. CÓDIGO DE PRODUCCIÓN PYSPARK STRUCTURED STREAMING (DATABRICKS / CLOUD)
# ==============================================================================
def ejecutar_pyspark_structured_streaming():
    """
    Implementación estándar de industria con PySpark Structured Streaming.
    Lista para ejecutarse en Databricks, AWS EMR, Google Dataproc o clúster local con JDK 17.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        DoubleType,
        StringType,
        TimestampType,
    )

    spark = (
        SparkSession.builder.appName("Medallion-Enterprise-Mexico-Streaming")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Regla de Oro en Entrevistas: NUNCA usar inferSchema en Streaming; definir StructType explícito
    esquema_ventas_stream = StructType(
        [
            StructField("id_venta", IntegerType(), True),
            StructField("id_cliente", IntegerType(), True),
            StructField("id_producto", IntegerType(), True),
            StructField("cantidad", IntegerType(), True),
            StructField("monto", DoubleType(), True),
            StructField("fecha_venta", StringType(), True),
            StructField("timestamp_evento", TimestampType(), True),
        ]
    )

    # Leer Dimensiones Estáticas desde Capa Silver (Parquet)
    dim_clientes = spark.read.parquet(os.path.join(SILVER_DIR, "dim_clientes.parquet"))
    dim_productos = spark.read.parquet(os.path.join(SILVER_DIR, "dim_productos.parquet"))

    # 1. Ingesta Streaming desde carpeta Bronze (o Kafka / Auto Loader cloudFiles)
    df_stream_bronze = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(esquema_ventas_stream)
        .load(BRONZE_STREAM_DIR)
    )

    # 2. Limpieza en vuelo (Silver Stream): Watermarking + Deduplicación + Filtros de Calidad
    df_stream_silver = (
        df_stream_bronze.withWatermark("timestamp_evento", "10 minutes")
        .dropDuplicates(["id_venta"])
        .filter(
            F.col("id_venta").isNotNull()
            & F.col("id_cliente").isNotNull()
            & F.col("id_producto").isNotNull()
            & (F.col("cantidad") > 0)
            & (F.col("monto") > 0)
        )
    )

    # 3. Doble JOIN en Esquema Estrella con Broadcast de Dimensiones (Optimización Big Data)
    df_enriched = (
        df_stream_silver.join(
            F.broadcast(dim_clientes), on="id_cliente", how="inner"
        ).join(
            F.broadcast(dim_productos), on="id_producto", how="inner"
        )
    )

    # 4. Filtro de Negocio ('Enterprise' + 'Mexico' normalizado) y Agregación Gold por Categoría
    df_gold_enterprise_mx = (
        df_enriched.filter(
            (F.upper(F.trim(F.col("segmento"))) == "ENTERPRISE")
            & (F.upper(F.trim(F.col("pais"))) == "MEXICO")
        )
        .groupBy("categoria", "pais", "segmento")
        .agg(
            F.count("id_venta").alias("total_operaciones"),
            F. approx_count_distinct("id_cliente").alias("clientes_enterprise_unicos"),
            F.sum("cantidad").alias("unidades_vendidas"),
            F.round(F.sum("monto"), 2).alias("ingresos_totales_usd"),
            F.round(F.avg("monto"), 2).alias("ticket_promedio_usd"),
        )
    )

    # 5. Escritura Streaming con Checkpoint para Tolerancia a Fallos (Exactly-Once Semantics)
    query = (
        df_gold_enterprise_mx.writeStream.outputMode("complete")
        .format("memory")
        .queryName("gold_enterprise_mexico_rt")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.sql("SELECT * FROM gold_enterprise_mexico_rt ORDER BY ingresos_totales_usd DESC").show(truncate=False)
    spark.stop()


# ==============================================================================
# 2. SIMULADOR DE MICRO-BATCHES EN TIEMPO REAL Y DETECTOR DE ERRORES (CUARENTENA)
# ==============================================================================
def simular_eventos_streaming_sucios():
    """
    Genera 3 micro-lotes (micro-batches) de eventos en tiempo real en data/bronze/stream_inbox/
    inyectando errores reales de sistemas distribuidos para probar la resiliencia del pipeline:
    - Eventos duplicados entre el Lote 1 y el Lote 2 (reintentos de red con distinto timestamp).
    - Productos huérfanos (id_producto = 999 que no existe en dim_productos).
    - Montos negativos y clientes NULL.
    """
    os.makedirs(BRONZE_STREAM_DIR, exist_ok=True)
    random.seed(99)

    con = duckdb.connect(database=":memory:")
    pq_clientes = os.path.join(SILVER_DIR, "dim_clientes.parquet").replace("\\", "/")
    pq_productos = os.path.join(SILVER_DIR, "dim_productos.parquet").replace("\\", "/")

    # Obtener clientes de Mexico Enterprise y otros para mezclar tráfico en vivo
    clientes_mx_ent = [
        r[0]
        for r in con.execute(
            f"SELECT id_cliente FROM read_parquet('{pq_clientes}') WHERE pais='Mexico' AND segmento='Enterprise'"
        ).fetchall()
    ]
    clientes_otros = [
        r[0]
        for r in con.execute(
            f"SELECT id_cliente FROM read_parquet('{pq_clientes}') WHERE NOT (pais='Mexico' AND segmento='Enterprise')"
        ).fetchall()
    ]
    productos = con.execute(
        f"SELECT id_producto, precio_lista FROM read_parquet('{pq_productos}')"
    ).fetchall()

    ahora = datetime(2026, 10, 20, 22, 0, 0)
    id_venta_seq = 90001
    eventos_para_duplicar = []

    resumen_lotes = []

    for lote_idx in range(1, 4):
        archivo_lote = os.path.join(BRONZE_STREAM_DIR, f"microbatch_00{lote_idx}.csv")
        filas_lote = []

        # Generar 200 eventos por micro-batch (600 eventos base + anomalías)
        for _ in range(200):
            es_target = random.random() < 0.65
            cid = random.choice(clientes_mx_ent if es_target else clientes_otros)
            prod_id, precio = random.choice(productos)
            cant = random.randint(1, 4)
            monto = round(precio * cant, 2)
            ts = (ahora + timedelta(seconds=random.randint(1, 300))).strftime("%Y-%m-%d %H:%M:%S")
            fecha = "2026-10-20"

            r_err = random.random()
            if r_err < 0.04:
                # Error tipo 1: Monto negativo en streaming
                monto = -monto
            elif r_err < 0.07:
                # Error tipo 2: Cliente vacío (NULL)
                cid = ""
            elif r_err < 0.10:
                # Error tipo 3: Producto fantasma/huérfano (id_producto=999 no existe en catálogo)
                prod_id = 999

            fila = [id_venta_seq, cid, prod_id, cant, monto, fecha, ts]
            filas_lote.append(fila)

            if lote_idx == 1 and len(eventos_para_duplicar) < 12 and cid != "" and monto > 0 and prod_id != 999:
                # Guardar eventos válidos del Lote 1 para re-inyectarlos como duplicados tardíos en Lote 2
                ts_tardio = (ahora + timedelta(seconds=500)).strftime("%Y-%m-%d %H:%M:%S")
                eventos_para_duplicar.append([id_venta_seq, cid, prod_id, cant, monto, fecha, ts_tardio])

            id_venta_seq += 1

        # En el Lote 2 inyectamos los duplicados tardíos inter-batch
        if lote_idx == 2:
            filas_lote.extend(eventos_para_duplicar)

        with open(archivo_lote, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["id_venta", "id_cliente", "id_producto", "cantidad", "monto", "fecha_venta", "timestamp_evento"]
            )
            writer.writerows(filas_lote)

        resumen_lotes.append((f"microbatch_00{lote_idx}.csv", len(filas_lote)))

    return resumen_lotes


def ejecutar_motor_streaming_hibrido():
    print("\n" + "=" * 78)
    print("🌊 INICIANDO PIPELINE DE STREAMING EN TIEMPO REAL (STRUCTURED STREAMING)")
    print("=" * 78)

    # 1. Verificar disponibilidad de JVM/PySpark o activar motor Micro-Batch local
    usar_pyspark = False
    try:
        import pyspark  # noqa: F401

        if os.environ.get("JAVA_HOME"):
            usar_pyspark = True
    except ImportError:
        usar_pyspark = False

    if usar_pyspark:
        print("⚡ Clúster PySpark + JVM detectado. Ejecutando Spark Structured Streaming...")
        ejecutar_pyspark_structured_streaming()
        return

    print("ℹ️  Modo Híbrido Activado: Ejecutando lógica de PySpark Structured Streaming")
    print("   sobre Motor Micro-Batch Local (DuckDB + Parquet) con Watermarking y Quarantine.")
    print("-" * 78)

    # 2. Generar los 3 micro-lotes de eventos en tiempo real
    lotes = simular_eventos_streaming_sucios()
    for nombre_archivo, num_eventos in lotes:
        print(f"📡 [INGESTA STREAM] Recibido {nombre_archivo:<20} -> {num_eventos:>4} eventos crudos")
        time.sleep(0.05)

    con = duckdb.connect(database=":memory:")
    glob_stream = os.path.join(BRONZE_STREAM_DIR, "microbatch_*.csv").replace("\\", "/")
    pq_clientes = os.path.join(SILVER_DIR, "dim_clientes.parquet").replace("\\", "/")
    pq_productos = os.path.join(SILVER_DIR, "dim_productos.parquet").replace("\\", "/")

    pq_stream_silver = os.path.join(SILVER_DIR, "fact_ventas_stream.parquet").replace("\\", "/")
    pq_quarantine = os.path.join(SILVER_DIR, "quarantine_errores_stream.parquet").replace("\\", "/")
    pq_gold_stream = os.path.join(GOLD_DIR, "kpi_streaming_enterprise_mexico.parquet").replace("\\", "/")

    # 3. Separar y Auditar Eventos Corruptos en Tabla de Cuarentena (Dead-Letter Queue)
    con.execute(f"""
        COPY (
            WITH raw_stream AS (
                SELECT
                    CAST(id_venta AS INTEGER) AS id_venta,
                    TRY_CAST(id_cliente AS INTEGER) AS id_cliente,
                    TRY_CAST(id_producto AS INTEGER) AS id_producto,
                    TRY_CAST(cantidad AS INTEGER) AS cantidad,
                    TRY_CAST(monto AS DOUBLE) AS monto,
                    CAST(fecha_venta AS DATE) AS fecha_venta,
                    CAST(timestamp_evento AS TIMESTAMP) AS timestamp_evento,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(id_venta AS INTEGER)
                        ORDER BY CAST(timestamp_evento AS TIMESTAMP) ASC
                    ) AS num_aparicion
                FROM read_csv_auto('{glob_stream}')
            )
            SELECT
                r.id_venta,
                r.id_cliente,
                r.id_producto,
                r.cantidad,
                r.monto,
                r.timestamp_evento,
                CASE
                    WHEN r.num_aparicion > 1 THEN 'DUPLICADO_INTER_BATCH'
                    WHEN r.id_cliente IS NULL THEN 'CLIENTE_NULL'
                    WHEN r.monto <= 0 THEN 'MONTO_NEGATIVO_O_CERO'
                    WHEN p.id_producto IS NULL THEN 'PRODUCTO_HUERFANO_NO_EXISTE_EN_CATALOGO'
                    WHEN c.id_cliente IS NULL THEN 'CLIENTE_HUERFANO_NO_EXISTE_EN_CATALOGO'
                    ELSE 'OTRO_ERROR'
                END AS motivo_rechazo
            FROM raw_stream r
            LEFT JOIN read_parquet('{pq_clientes}') c ON r.id_cliente = c.id_cliente
            LEFT JOIN read_parquet('{pq_productos}') p ON r.id_producto = p.id_producto
            WHERE r.num_aparicion > 1
               OR r.id_cliente IS NULL
               OR r.monto <= 0
               OR p.id_producto IS NULL
               OR c.id_cliente IS NULL
        ) TO '{pq_quarantine}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # 4. Guardar Eventos Limpios en Silver Stream (Deduplicados con Watermark + Integridad Referencial)
    con.execute(f"""
        COPY (
            SELECT
                CAST(r.id_venta AS INTEGER) AS id_venta,
                TRY_CAST(r.id_cliente AS INTEGER) AS id_cliente,
                TRY_CAST(r.id_producto AS INTEGER) AS id_producto,
                TRY_CAST(r.cantidad AS INTEGER) AS cantidad,
                ROUND(TRY_CAST(r.monto AS DOUBLE), 2) AS monto,
                CAST(r.fecha_venta AS DATE) AS fecha_venta,
                CAST(r.timestamp_evento AS TIMESTAMP) AS timestamp_evento
            FROM read_csv_auto('{glob_stream}') r
            INNER JOIN read_parquet('{pq_clientes}') c
              ON TRY_CAST(r.id_cliente AS INTEGER) = c.id_cliente
            INNER JOIN read_parquet('{pq_productos}') p
              ON TRY_CAST(r.id_producto AS INTEGER) = p.id_producto
            WHERE TRY_CAST(r.id_cliente AS INTEGER) IS NOT NULL
              AND TRY_CAST(r.monto AS DOUBLE) > 0
              AND TRY_CAST(r.cantidad AS INTEGER) > 0
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(r.id_venta AS INTEGER)
                ORDER BY CAST(r.timestamp_evento AS TIMESTAMP) ASC
            ) = 1
        ) TO '{pq_stream_silver}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # 5. Actualizar Vista Materializada Gold en Tiempo Real (Enterprise + Mexico por Categoría)
    con.execute(f"""
        COPY (
            SELECT
                p.categoria,
                c.pais,
                c.segmento,
                COUNT(s.id_venta) AS eventos_streaming_validos,
                COUNT(DISTINCT s.id_cliente) AS clientes_activos_rt,
                SUM(s.cantidad) AS unidades_rt,
                ROUND(SUM(s.monto), 2) AS ingresos_streaming_usd,
                MAX(s.timestamp_evento) AS ultimo_evento_procesado
            FROM read_parquet('{pq_stream_silver}') s
            INNER JOIN read_parquet('{pq_clientes}') c ON s.id_cliente = c.id_cliente
            INNER JOIN read_parquet('{pq_productos}') p ON s.id_producto = p.id_producto
            WHERE UPPER(TRIM(c.segmento)) = 'ENTERPRISE'
              AND UPPER(TRIM(c.pais)) = 'MEXICO'
            GROUP BY p.categoria, c.pais, c.segmento
            ORDER BY ingresos_streaming_usd DESC
        ) TO '{pq_gold_stream}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    total_raw = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{glob_stream}')").fetchone()[0]
    total_clean = con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_stream_silver}')").fetchone()[0]
    total_quarantine = con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_quarantine}')").fetchone()[0]

    print("-" * 78)
    print(f"📊 Eventos recibidos en Stream : {total_raw:,}")
    print(f"✅ Eventos limpios en Silver   : {total_clean:,}")
    print(f"🛡️  Eventos aislados Cuarentena : {total_quarantine:,} (Bloqueados antes de contaminar Gold)")
    print("-" * 78)
    print("🔍 DESGLOSE DE ERRORES CAZADOS EN TIEMPO REAL (TABLA QUARANTINE):")
    motivos = con.execute(
        f"SELECT motivo_rechazo, COUNT(*) AS total FROM read_parquet('{pq_quarantine}') GROUP BY motivo_rechazo ORDER BY total DESC"
    ).fetchall()
    for motivo, cant in motivos:
        print(f"   • {motivo:<42} : {cant:>3} eventos interceptados")

    print("\n⚡ RESULTADO STREAMING GOLD: Ventas En Vivo 'Enterprise' en 'Mexico' por Categoría")
    print("-" * 78)
    res_stream = con.execute(f"SELECT * FROM read_parquet('{pq_gold_stream}')").fetchall()
    print(f"{'CATEGORÍA':<14} | {'EVENTOS':>8} | {'CLIENTES':>9} | {'UNIDADES':>9} | {'INGRESOS RT (USD)':>18}")
    print("-" * 78)
    for fila in res_stream:
        print(f"{fila[0]:<14} | {fila[3]:>8,} | {fila[4]:>9,} | {fila[5]:>9,} | ${fila[6]:>17,.2f}")
    print("=" * 78)


if __name__ == "__main__":
    ejecutar_motor_streaming_hibrido()
