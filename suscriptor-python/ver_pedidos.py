"""
Muestra los pedidos guardados en pedidos.db (sin instalar nada más).

Ejecutar desde la carpeta suscriptor-python:
    python ver_pedidos.py
"""

import sqlite3
from pathlib import Path

RUTA_BASE_DATOS = Path(__file__).resolve().parent / "pedidos.db"

if not RUTA_BASE_DATOS.is_file():
    raise SystemExit("Todavía no existe pedidos.db: ejecuta primero suscriptor.py.")

with sqlite3.connect(RUTA_BASE_DATOS) as conexion:
    filas = conexion.execute(
        "SELECT id, producto, cantidad, recibido_en FROM pedidos ORDER BY id"
    ).fetchall()

print(f"{'id':>4}  {'producto':<30} {'cantidad':>8}  recibido_en")
print("-" * 66)
for id_pedido, producto, cantidad, recibido_en in filas:
    print(f"{id_pedido:>4}  {producto:<30} {cantidad:>8}  {recibido_en}")
print(f"\nTotal: {len(filas)} pedido(s)")
