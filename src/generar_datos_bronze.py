"""
Paso 1: Simulador de Ingesta de Datos (Capa Bronze 🥉)
------------------------------------------------------
Genera 50,000 registros de ventas crudas (CSV) con problemas reales del mundo empresarial:
- Filas duplicadas (errores de red al cobrar)
- Montos negativos (cancelaciones / devoluciones como el caso de Marta -$50)
- Valores vacíos (NULL)
Y crea los catálogos (Dimensiones) de Clientes y Productos para practicar JOINs por ID.
"""

import csv
import os
import random
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")


def generar_capa_bronze():
    os.makedirs(BRONZE_DIR, exist_ok=True)
    random.seed(42)

    # 1. Crear Catálogo de Productos (dim_productos) -> Llave única: id_producto
    productos = [
        (201, "Laptop Pro 16", "Tecnologia", 1200.00),
        (202, "Monitor UltraWide 34", "Tecnologia", 450.00),
        (203, "Teclado Mecanico", "Accesorios", 120.00),
        (204, "Mouse Ergonomico", "Accesorios", 80.00),
        (205, "Silla Gamer Ergonomica", "Muebles", 350.00),
        (206, "Audifonos Noise Cancelling", "Audio", 250.00),
    ]
    ruta_productos = os.path.join(BRONZE_DIR, "productos_raw.csv")
    with open(ruta_productos, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_producto", "nombre_producto", "categoria", "precio_lista"])
        writer.writerows(productos)

    # 2. Crear Catálogo de Clientes (dim_clientes) -> Llave única: id_cliente
    paises = ["Mexico", "Colombia", "España", "Argentina", "Chile", "Estados Unidos"]
    nombres_base = ["Ana", "Luis", "Marta", "Pedro", "Carlos", "Sofia", "Diego", "Valeria", "Jorge", "Elena"]
    segmentos = ["Enterprise", "PyME", "Consumidor"]

    clientes = []
    for cid in range(1001, 1501):  # 500 clientes únicos
        nombre = f"{random.choice(nombres_base)}_{cid}"
        pais = random.choice(paises)
        segmento = random.choice(segmentos)
        clientes.append((cid, nombre, pais, segmento))

    ruta_clientes = os.path.join(BRONZE_DIR, "clientes_raw.csv")
    with open(ruta_clientes, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_cliente", "nombre_cliente", "pais", "segmento"])
        writer.writerows(clientes)

    # 3. Crear Tabla de Hechos Cruda (ventas_raw.csv) con 50,000 filas + "basura" intencional
    ruta_ventas = os.path.join(BRONZE_DIR, "ventas_raw.csv")
    fecha_inicio = datetime(2026, 1, 1)

    filas_totales = 0
    duplicados_inyectados = 0
    negativos_inyectados = 0
    nulos_inyectados = 0

    with open(ruta_ventas, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_venta", "id_cliente", "id_producto", "cantidad", "monto", "fecha_venta"])

        for vid in range(1, 50001):
            id_cliente = random.choice(clientes)[0]
            prod = random.choice(productos)
            id_producto = prod[0]
            precio = prod[3]
            cantidad = random.randint(1, 5)
            monto = round(precio * cantidad, 2)
            fecha = (fecha_inicio + timedelta(days=random.randint(0, 280))).strftime("%Y-%m-%d")

            # Inyectar 3% de ventas negativas (cancelaciones/devoluciones)
            if random.random() < 0.03:
                monto = -monto
                negativos_inyectados += 1

            # Inyectar 2% de clientes vacíos (NULL)
            if random.random() < 0.02:
                id_cliente_str = ""
                nulos_inyectados += 1
            else:
                id_cliente_str = str(id_cliente)

            fila = [vid, id_cliente_str, id_producto, cantidad, monto, fecha]
            writer.writerow(fila)
            filas_totales += 1

            # Inyectar 4% de filas duplicadas exactas (fallo de red al cobrar)
            if random.random() < 0.04:
                writer.writerow(fila)
                filas_totales += 1
                duplicados_inyectados += 1

    peso_kb = os.path.getsize(ruta_ventas) / 1024
    print("=" * 65)
    print("🥉 CAPA BRONZE GENERADA EXITOSAMENTE")
    print("=" * 65)
    print(f"📂 Archivo creado : {ruta_ventas}")
    print(f"📊 Filas totales  : {filas_totales:,} registros crudos")
    print(f"⚖️  Peso en CSV    : {peso_kb:,.1f} KB")
    print("-" * 65)
    print("⚠️  'Basura' simulada en Bronze que nuestro Pipeline limpiará:")
    print(f"   • Duplicados exactos : {duplicados_inyectados:,} filas")
    print(f"   • Montos negativos   : {negativos_inyectados:,} devoluciones")
    print(f"   • Clientes sin ID    : {nulos_inyectados:,} valores vacíos (NULL)")
    print("=" * 65)


if __name__ == "__main__":
    generar_capa_bronze()
