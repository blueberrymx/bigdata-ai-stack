"""
Paso 2: Motor Big Data Medallion (Bronze 🥉 -> Silver 🥈 -> Gold 🥇)
-------------------------------------------------------------------
Este script usa DuckDB (Motor OLAP Columnar) y SQL para:
1. Leer los CSV sucios de la capa Bronze.
2. Limpiar duplicados por llave primaria (QUALIFY ROW_NUMBER() OVER PARTITION BY id_venta),
   filtrar errores (WHERE monto > 0 AND id_cliente IS NOT NULL), validar integridad referencial
   y guardar todo en formato APACHE PARQUET (.parquet) en la capa Silver.
3. Unir las tablas en Esquema Estrella (Star Schema) con JOIN usando sus llaves únicas
   (id_cliente e id_producto) y agrupar con GROUP BY para crear los 3 reportes ejecutivos
   en la capa Gold (incluyendo el nuevo reporte Enterprise en Mexico por Categoría).
"""

import os
import time
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")


def ejecutar_pipeline():
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)

    inicio = time.time()
    con = duckdb.connect(database=":memory:")

    # Rutas Bronze (CSV)
    csv_ventas = os.path.join(BRONZE_DIR, "ventas_raw.csv").replace("\\", "/")
    csv_clientes = os.path.join(BRONZE_DIR, "clientes_raw.csv").replace("\\", "/")
    csv_productos = os.path.join(BRONZE_DIR, "productos_raw.csv").replace("\\", "/")

    # Rutas Silver (Parquet)
    pq_ventas = os.path.join(SILVER_DIR, "fact_ventas.parquet").replace("\\", "/")
    pq_clientes = os.path.join(SILVER_DIR, "dim_clientes.parquet").replace("\\", "/")
    pq_productos = os.path.join(SILVER_DIR, "dim_productos.parquet").replace("\\", "/")

    # Rutas Gold (Parquet)
    pq_gold_pais = os.path.join(GOLD_DIR, "kpi_ventas_por_pais.parquet").replace("\\", "/")
    pq_gold_prod = os.path.join(GOLD_DIR, "kpi_ventas_por_producto.parquet").replace("\\", "/")
    pq_gold_ent_mx = os.path.join(GOLD_DIR, "kpi_ventas_enterprise_mexico.parquet").replace("\\", "/")

    print("\n" + "=" * 78)
    print("🥈 ETAPA 1: TRANSFORMACIÓN BRONZE (CSV) -> SILVER (PARQUET LIMPIO Y AUDITADO)")
    print("=" * 78)

    # 1. Convertir Dimensiones a Parquet (normalizando textos con TRIM y deduplicando por PK)
    con.execute(f"""
        COPY (
            SELECT
                CAST(id_cliente AS INTEGER) AS id_cliente,
                TRIM(CAST(nombre_cliente AS VARCHAR)) AS nombre_cliente,
                TRIM(CAST(pais AS VARCHAR)) AS pais,
                TRIM(CAST(segmento AS VARCHAR)) AS segmento
            FROM read_csv_auto('{csv_clientes}')
            WHERE id_cliente IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(id_cliente AS INTEGER) ORDER BY nombre_cliente) = 1
        ) TO '{pq_clientes}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    con.execute(f"""
        COPY (
            SELECT
                CAST(id_producto AS INTEGER) AS id_producto,
                TRIM(CAST(nombre_producto AS VARCHAR)) AS nombre_producto,
                TRIM(CAST(categoria AS VARCHAR)) AS categoria,
                CAST(precio_lista AS DOUBLE) AS precio_lista
            FROM read_csv_auto('{csv_productos}')
            WHERE id_producto IS NOT NULL
              AND CAST(precio_lista AS DOUBLE) > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(id_producto AS INTEGER) ORDER BY nombre_producto) = 1
        ) TO '{pq_productos}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # 2. Limpiar Tabla de Hechos (fact_ventas) con SQL y guardar en Parquet
    # Mejora de Ingeniería:
    # - QUALIFY ROW_NUMBER() OVER (PARTITION BY id_venta ...) garantiza 0 id_venta duplicados
    #   incluso si un reintento de red trajo diferente fecha o monto.
    # - Validación de Integridad Referencial contra dim_clientes y dim_productos.
    con.execute(f"""
        COPY (
            SELECT
                CAST(v.id_venta AS INTEGER) AS id_venta,
                CAST(v.id_cliente AS INTEGER) AS id_cliente,
                CAST(v.id_producto AS INTEGER) AS id_producto,
                CAST(v.cantidad AS INTEGER) AS cantidad,
                ROUND(CAST(v.monto AS DOUBLE), 2) AS monto,
                CAST(v.fecha_venta AS DATE) AS fecha_venta
            FROM read_csv_auto('{csv_ventas}') v
            INNER JOIN read_parquet('{pq_clientes}') c
              ON CAST(v.id_cliente AS INTEGER) = c.id_cliente
            INNER JOIN read_parquet('{pq_productos}') p
              ON CAST(v.id_producto AS INTEGER) = p.id_producto
            WHERE v.id_venta IS NOT NULL
              AND v.id_cliente IS NOT NULL
              AND v.id_producto IS NOT NULL
              AND CAST(v.cantidad AS INTEGER) > 0
              AND CAST(v.monto AS DOUBLE) > 0
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CAST(v.id_venta AS INTEGER)
                ORDER BY CAST(v.fecha_venta AS DATE) DESC
            ) = 1
        ) TO '{pq_ventas}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    filas_bronze = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_ventas}')").fetchone()[0]
    filas_silver = con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_ventas}')").fetchone()[0]
    peso_csv_kb = os.path.getsize(csv_ventas) / 1024
    peso_pq_kb = os.path.getsize(pq_ventas) / 1024
    ahorro_pct = (1 - (peso_pq_kb / peso_csv_kb)) * 100

    print(f"✅ Filas en Bronze (Sucias)  : {filas_bronze:,}")
    print(f"✅ Filas en Silver (Limpias) : {filas_silver:,} (Se eliminaron {filas_bronze - filas_silver:,} filas basura)")
    print(f"📦 Peso en CSV (Bronze)      : {peso_csv_kb:,.1f} KB")
    print(f"🚀 Peso en PARQUET (Silver)  : {peso_pq_kb:,.1f} KB (¡{ahorro_pct:.1f}% más ligero gracias a Parquet!)")

    print("\n" + "=" * 78)
    print("🥇 ETAPA 2: SILVER -> GOLD (ESQUEMA ESTRELLA CON JOIN POR ID + GROUP BY)")
    print("=" * 78)

    # Reporte Gold 1: Ventas por País (Uniendo fact_ventas + dim_clientes por id_cliente)
    con.execute(f"""
        COPY (
            SELECT
                c.pais,
                COUNT(v.id_venta) AS total_operaciones,
                COUNT(DISTINCT v.id_cliente) AS clientes_unicos,
                ROUND(SUM(v.monto), 2) AS ingresos_totales_usd,
                ROUND(AVG(v.monto), 2) AS ticket_promedio_usd
            FROM read_parquet('{pq_ventas}') v
            INNER JOIN read_parquet('{pq_clientes}') c
              ON v.id_cliente = c.id_cliente
            GROUP BY c.pais
            ORDER BY ingresos_totales_usd DESC
        ) TO '{pq_gold_pais}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Reporte Gold 2: Ventas por Producto (Uniendo fact_ventas + dim_productos por id_producto)
    con.execute(f"""
        COPY (
            SELECT
                p.id_producto,
                p.nombre_producto,
                p.categoria,
                SUM(v.cantidad) AS unidades_vendidas,
                ROUND(SUM(v.monto), 2) AS ingresos_totales_usd
            FROM read_parquet('{pq_ventas}') v
            INNER JOIN read_parquet('{pq_productos}') p
              ON v.id_producto = p.id_producto
            GROUP BY p.id_producto, p.nombre_producto, p.categoria
            ORDER BY ingresos_totales_usd DESC
        ) TO '{pq_gold_prod}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Reporte Gold 3 (NUEVO PEDIDO DEL CLIENTE):
    # "Únicamente las ventas del segmento 'Enterprise' en 'Mexico', agrupadas por categoría de producto"
    # Requiere DOBLE JOIN en Esquema Estrella:
    #   fact_ventas (v) -> dim_clientes (c) por id_cliente (para filtrar segmento='Enterprise' y pais='Mexico')
    #   fact_ventas (v) -> dim_productos (p) por id_producto (para agrupar por p.categoria)
    con.execute(f"""
        COPY (
            SELECT
                p.categoria,
                c.pais,
                c.segmento,
                COUNT(v.id_venta) AS total_operaciones,
                COUNT(DISTINCT v.id_cliente) AS clientes_enterprise_unicos,
                SUM(v.cantidad) AS unidades_vendidas,
                ROUND(SUM(v.monto), 2) AS ingresos_totales_usd,
                ROUND(AVG(v.monto), 2) AS ticket_promedio_usd,
                ROUND(SUM(v.monto) * 100.0 / SUM(SUM(v.monto)) OVER (), 2) AS share_categoria_pct
            FROM read_parquet('{pq_ventas}') v
            INNER JOIN read_parquet('{pq_clientes}') c
              ON v.id_cliente = c.id_cliente
            INNER JOIN read_parquet('{pq_productos}') p
              ON v.id_producto = p.id_producto
            WHERE UPPER(TRIM(c.segmento)) = 'ENTERPRISE'
              AND UPPER(TRIM(c.pais)) = 'MEXICO'
            GROUP BY p.categoria, c.pais, c.segmento
            ORDER BY ingresos_totales_usd DESC
        ) TO '{pq_gold_ent_mx}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    print("\n📊 RESULTADO CAPA GOLD 1: Ingresos por País (JOIN por id_cliente)")
    print("-" * 78)
    res_pais = con.execute(f"SELECT * FROM read_parquet('{pq_gold_pais}')").fetchall()
    print(f"{'PAÍS':<18} | {'OPERACIONES':>12} | {'CLIENTES':>10} | {'INGRESOS (USD)':>16}")
    print("-" * 78)
    for fila in res_pais:
        print(f"{fila[0]:<18} | {fila[1]:>12,} | {fila[2]:>10,} | ${fila[3]:>15,.2f}")

    print("\n💻 RESULTADO CAPA GOLD 2: Top Productos (JOIN por id_producto)")
    print("-" * 78)
    res_prod = con.execute(f"SELECT * FROM read_parquet('{pq_gold_prod}')").fetchall()
    print(f"{'ID':<5} | {'PRODUCTO':<26} | {'UNIDADES':>10} | {'INGRESOS (USD)':>16}")
    print("-" * 78)
    for fila in res_prod:
        print(f"{fila[0]:<5} | {fila[1]:<26} | {fila[3]:>10,} | ${fila[4]:>15,.2f}")

    print("\n🏢🇲🇽 RESULTADO CAPA GOLD 3 (NUEVO): Ventas 'Enterprise' en 'Mexico' por Categoría")
    print("    (Doble JOIN: fact_ventas ⋈ dim_clientes [id_cliente] ⋈ dim_productos [id_producto])")
    print("-" * 78)
    res_ent_mx = con.execute(f"SELECT * FROM read_parquet('{pq_gold_ent_mx}')").fetchall()
    print(
        f"{'CATEGORÍA':<14} | {'OPS':>6} | {'CLIENTES':>8} | {'UNIDADES':>9} | {'INGRESOS (USD)':>15} | {'SHARE':>7}"
    )
    print("-" * 78)
    total_ops_mx = 0
    total_ing_mx = 0.0
    for fila in res_ent_mx:
        total_ops_mx += fila[3]
        total_ing_mx += fila[6]
        print(
            f"{fila[0]:<14} | {fila[3]:>6,} | {fila[4]:>8,} | {fila[5]:>9,} | ${fila[6]:>14,.2f} | {fila[8]:>6.2f}%"
        )
    print("-" * 78)
    print(f"{'TOTAL MÉXICO':<14} | {total_ops_mx:>6,} | {'-':>8} | {'-':>9} | ${total_ing_mx:>14,.2f} | 100.00%")

    duracion = time.time() - inicio
    print("=" * 78)
    print(f"⚡ Pipeline Medallion completado en {duracion:.2f} segundos.")
    print("=" * 78)


if __name__ == "__main__":
    ejecutar_pipeline()
