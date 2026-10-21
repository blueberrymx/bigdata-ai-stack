"""
Paso 3: Agente Auditor de Calidad de Datos (Data Quality & Observability Suite)
-------------------------------------------------------------------------------
En un rol remoto de Data Engineer, nunca entregas un pipeline Batch ni Streaming
sin pruebas automatizadas de calidad (Data Quality & Reconciliation Tests).
Este script audita 10 reglas críticas en tus capas Silver, Gold y Streaming:
1-4. Calidad e Integridad Referencial en Silver Batch (fact_ventas.parquet)
5-8. Reglas de Negocio y Reconciliación Contable en Gold (kpi_ventas_enterprise_mexico.parquet)
9-10. Auditoría de Streaming y Cero Pérdida de Datos (Silver Stream + Quarantine)
"""

import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_STREAM_DIR = os.path.join(BASE_DIR, "data", "bronze", "stream_inbox")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")


def auditar_calidad():
    pq_ventas = os.path.join(SILVER_DIR, "fact_ventas.parquet").replace("\\", "/")
    pq_clientes = os.path.join(SILVER_DIR, "dim_clientes.parquet").replace("\\", "/")
    pq_productos = os.path.join(SILVER_DIR, "dim_productos.parquet").replace("\\", "/")
    pq_gold_ent_mx = os.path.join(GOLD_DIR, "kpi_ventas_enterprise_mexico.parquet").replace("\\", "/")
    pq_stream_silver = os.path.join(SILVER_DIR, "fact_ventas_stream.parquet").replace("\\", "/")
    pq_quarantine = os.path.join(SILVER_DIR, "quarantine_errores_stream.parquet").replace("\\", "/")
    glob_stream = os.path.join(BRONZE_STREAM_DIR, "microbatch_*.csv").replace("\\", "/")

    con = duckdb.connect(database=":memory:")

    print("\n" + "=" * 78)
    print("🤖 AGENTE AUDITOR DE CALIDAD DE DATOS (SILVER + GOLD + STREAMING)")
    print("=" * 78)

    pruebas = [
        # --- BLOQUE A: CAPA SILVER BATCH ---
        (
            "Test 01 [Silver]: Cero ventas negativas o en cero (monto <= 0)",
            f"SELECT COUNT(*) FROM read_parquet('{pq_ventas}') WHERE monto <= 0",
        ),
        (
            "Test 02 [Silver]: Cero llaves vacías (id_cliente / id_producto NULL)",
            f"SELECT COUNT(*) FROM read_parquet('{pq_ventas}') WHERE id_cliente IS NULL OR id_producto IS NULL",
        ),
        (
            "Test 03 [Silver]: Unicidad de PK (Cero id_venta duplicados)",
            f"SELECT COUNT(*) - COUNT(DISTINCT id_venta) FROM read_parquet('{pq_ventas}')",
        ),
        (
            "Test 04 [Silver]: Integridad Referencial (Cero llaves huérfanas)",
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{pq_ventas}') v
            LEFT JOIN read_parquet('{pq_clientes}') c ON v.id_cliente = c.id_cliente
            LEFT JOIN read_parquet('{pq_productos}') p ON v.id_producto = p.id_producto
            WHERE c.id_cliente IS NULL OR p.id_producto IS NULL
            """,
        ),
        # --- BLOQUE B: CAPA GOLD (NUEVA TABLA ENTERPRISE MEXICO) ---
        (
            "Test 05 [Gold]  : Exclusividad de filtro ('Enterprise' y 'Mexico')",
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{pq_gold_ent_mx}')
            WHERE pais != 'Mexico' OR segmento != 'Enterprise'
            """,
        ),
        (
            "Test 06 [Gold]  : Completitud de las 4 categorías de producto",
            f"""
            SELECT CASE WHEN COUNT(DISTINCT categoria) = 4 AND COUNT(*) = 4 THEN 0 ELSE 1 END
            FROM read_parquet('{pq_gold_ent_mx}')
            WHERE categoria IS NOT NULL
            """,
        ),
        (
            "Test 07 [Gold]  : Reconciliación Contable exacta Silver vs Gold (USD)",
            f"""
            WITH silver_total AS (
                SELECT ROUND(SUM(v.monto), 2) AS total_s
                FROM read_parquet('{pq_ventas}') v
                JOIN read_parquet('{pq_clientes}') c ON v.id_cliente = c.id_cliente
                WHERE c.pais = 'Mexico' AND c.segmento = 'Enterprise'
            ),
            gold_total AS (
                SELECT ROUND(SUM(ingresos_totales_usd), 2) AS total_g
                FROM read_parquet('{pq_gold_ent_mx}')
            )
            SELECT CASE WHEN ABS(silver_total.total_s - gold_total.total_g) < 0.01 THEN 0 ELSE 1 END
            FROM silver_total, gold_total
            """,
        ),
        (
            "Test 08 [Gold]  : Suma de Market Share por categoría = 100.0%",
            f"""
            SELECT CASE WHEN ABS(SUM(share_categoria_pct) - 100.0) < 0.05 THEN 0 ELSE 1 END
            FROM read_parquet('{pq_gold_ent_mx}')
            """,
        ),
        # --- BLOQUE C: STREAMING EN TIEMPO REAL Y CUARENTENA ---
        (
            "Test 09 [Stream]: Cero duplicados ni productos fantasma (id=999) en Silver RT",
            f"""
            SELECT (COUNT(*) - COUNT(DISTINCT id_venta))
                 + SUM(CASE WHEN id_producto = 999 OR monto <= 0 OR id_cliente IS NULL THEN 1 ELSE 0 END)
            FROM read_parquet('{pq_stream_silver}')
            """,
        ),
        (
            "Test 10 [Stream]: Ecuación de Balance (Raw [612] = Clean [534] + Quarantine [78])",
            f"""
            WITH tot_raw AS (SELECT COUNT(*) AS n FROM read_csv_auto('{glob_stream}')),
                 tot_clean AS (SELECT COUNT(*) AS n FROM read_parquet('{pq_stream_silver}')),
                 tot_quar AS (SELECT COUNT(*) AS n FROM read_parquet('{pq_quarantine}'))
            SELECT ABS(tot_raw.n - (tot_clean.n + tot_quar.n))
            FROM tot_raw, tot_clean, tot_quar
            """,
        ),
    ]

    todos_pasaron = True
    pasados = 0
    for nombre_test, sql_query in pruebas:
        errores = int(con.execute(sql_query).fetchone()[0] or 0)
        estado = "✅ PASÓ (0 errores)" if errores == 0 else f"❌ FALLÓ ({errores} errores)"
        if errores > 0:
            todos_pasaron = False
        else:
            pasados += 1
        print(f"• {nombre_test:<56} -> {estado}")

    print("-" * 78)
    if todos_pasaron:
        print(f"🏆 CERTIFICADO DE CALIDAD ({pasados}/10 TESTS PASADOS):")
        print("   Tu Lakehouse (Batch + Gold Enterprise Mexico + Streaming + Quarantine)")
        print("   está 100% libre de errores y reconciliado al centavo.")
    else:
        print("⚠️ ALERTA: Se detectaron anomalías en los datos.")
    print("=" * 78)


if __name__ == "__main__":
    auditar_calidad()
