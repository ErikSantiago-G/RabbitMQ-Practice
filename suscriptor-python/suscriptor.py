"""
Suscriptor 1 (Python): recibe pedidos de RabbitMQ y los guarda en SQLite.

Recorrido de cada mensaje:
    pedidos_exchange (fanout) -> cola pedidos_python -> este programa -> pedidos.db

Ejecutar desde la carpeta suscriptor-python, con el entorno virtual activado:
    python suscriptor.py
Detener con Ctrl+C.
"""

import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

import pika
from pika.exceptions import AMQPChannelError, AMQPConnectionError, ProbableAuthenticationError

# --- Nombres de la topología (deben coincidir con los otros programas) ---
EXCHANGE_PEDIDOS = "pedidos_exchange"
COLA_PYTHON = "pedidos_python"

# --- Rutas ---
CARPETA_SCRIPT = Path(__file__).resolve().parent
RUTA_BASE_DATOS = CARPETA_SCRIPT / "pedidos.db"
RUTAS_ARCHIVO_ENV = [Path.cwd() / ".env", CARPETA_SCRIPT.parent / ".env"]

SEGUNDOS_ENTRE_REINTENTOS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("suscriptor-python")
# pika registra muchos detalles internos (incluso trazas largas al reconectar).
# Los ocultamos: nuestro programa ya informa de cada problema con un mensaje claro.
logging.getLogger("pika").setLevel(logging.CRITICAL)


class MensajeInvalido(Exception):
    """El mensaje no cumple el contrato {"producto": texto, "cantidad": entero > 0}."""


# ----------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------
def leer_archivo_env():
    """Lee el primer .env que encuentre y devuelve un diccionario."""
    for ruta in RUTAS_ARCHIVO_ENV:
        if ruta.is_file():
            valores = {}
            for linea in ruta.read_text(encoding="utf-8").splitlines():
                linea = linea.strip()
                if not linea or linea.startswith("#") or "=" not in linea:
                    continue
                clave, valor = linea.split("=", 1)
                valores[clave.strip()] = valor.strip()
            return valores
    return {}


def cargar_configuracion():
    """Prioridad: variable de entorno > archivo .env > valor por defecto."""
    archivo = leer_archivo_env()

    def valor(nombre, por_defecto=None):
        return os.environ.get(nombre) or archivo.get(nombre) or por_defecto

    usuario = valor("RABBITMQ_USER")
    contrasena = valor("RABBITMQ_PASSWORD")
    if not usuario or not contrasena:
        raise SystemExit(
            "Faltan RABBITMQ_USER o RABBITMQ_PASSWORD. "
            "Revisa el archivo .env en la raíz del proyecto."
        )

    puerto_texto = valor("RABBITMQ_PORT", "5672")
    if not puerto_texto.isdigit():
        raise SystemExit(f"RABBITMQ_PORT debe ser un número entero, pero vale: {puerto_texto}")

    return pika.ConnectionParameters(
        host=valor("RABBITMQ_HOST", "localhost"),
        port=int(puerto_texto),
        virtual_host=valor("RABBITMQ_VHOST", "/"),
        credentials=pika.PlainCredentials(usuario, contrasena),
        heartbeat=60,
        connection_attempts=1,
    )


# ----------------------------------------------------------------------
# Base de datos
# ----------------------------------------------------------------------
def abrir_base_datos():
    """Crea (si no existen) el archivo pedidos.db y la tabla pedidos."""
    conexion = sqlite3.connect(RUTA_BASE_DATOS)
    conexion.execute(
        """
        CREATE TABLE IF NOT EXISTS pedidos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            producto    TEXT    NOT NULL,
            cantidad    INTEGER NOT NULL CHECK (cantidad > 0),
            mensaje_id  TEXT    UNIQUE,
            recibido_en TEXT    NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )
    conexion.commit()
    return conexion


def guardar_pedido(conexion, producto, cantidad, mensaje_id):
    """
    Inserta el pedido. Devuelve True si se insertó y False si era un duplicado.

    INSERT OR IGNORE + mensaje_id UNIQUE: si RabbitMQ entrega dos veces el mismo
    mensaje (por ejemplo, tras una caída antes del ACK), no se guarda dos veces.
    """
    cursor = conexion.execute(
        "INSERT OR IGNORE INTO pedidos (producto, cantidad, mensaje_id) VALUES (?, ?, ?)",
        (producto, cantidad, mensaje_id),
    )
    conexion.commit()
    return cursor.rowcount == 1


# ----------------------------------------------------------------------
# Mensajes
# ----------------------------------------------------------------------
def interpretar_mensaje(cuerpo):
    """Convierte los bytes recibidos en (producto, cantidad) o lanza MensajeInvalido."""
    try:
        datos = json.loads(cuerpo.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MensajeInvalido(f"no es JSON UTF-8 válido ({error})") from error

    if not isinstance(datos, dict):
        raise MensajeInvalido("el JSON debe ser un objeto {...}")

    producto = datos.get("producto")
    cantidad = datos.get("cantidad")

    if not isinstance(producto, str) or not producto.strip():
        raise MensajeInvalido("falta 'producto' o no es un texto")
    # En Python, True es un int; lo excluimos explícitamente.
    if isinstance(cantidad, bool) or not isinstance(cantidad, int) or cantidad <= 0:
        raise MensajeInvalido("'cantidad' debe ser un entero mayor que cero")

    return producto.strip(), cantidad


def crear_procesador(base_datos):
    """Devuelve la función que pika llamará por cada mensaje recibido."""

    def procesar_mensaje(canal, entrega, propiedades, cuerpo):
        etiqueta = entrega.delivery_tag

        try:
            producto, cantidad = interpretar_mensaje(cuerpo)
        except MensajeInvalido as error:
            log.warning("Mensaje inválido descartado: %s. Contenido: %r", error, cuerpo[:200])
            # NACK sin reencolar: reintentarlo no lo arreglaría.
            canal.basic_nack(delivery_tag=etiqueta, requeue=False)
            return

        try:
            insertado = guardar_pedido(base_datos, producto, cantidad, propiedades.message_id)
        except sqlite3.Error as error:
            log.error("No se pudo guardar en SQLite: %s. El mensaje vuelve a la cola.", error)
            # Sin ACK: lo devolvemos a la cola y paramos para no perder el pedido.
            canal.basic_nack(delivery_tag=etiqueta, requeue=True)
            canal.stop_consuming()
            return

        # ACK solo DESPUÉS de guardar: si el programa cae antes, RabbitMQ lo reenvía.
        canal.basic_ack(delivery_tag=etiqueta)
        if insertado:
            log.info("Pedido guardado: %s x %d", producto, cantidad)
        else:
            log.info("Pedido duplicado ignorado (mensaje_id=%s)", propiedades.message_id)

    return procesar_mensaje


# ----------------------------------------------------------------------
# Conexión y bucle principal
# ----------------------------------------------------------------------
def declarar_topologia(canal):
    """Idempotente: crea exchange, cola y binding si no existen."""
    canal.exchange_declare(exchange=EXCHANGE_PEDIDOS, exchange_type="fanout", durable=True)
    canal.queue_declare(queue=COLA_PYTHON, durable=True)
    canal.queue_bind(queue=COLA_PYTHON, exchange=EXCHANGE_PEDIDOS)


def consumir(parametros, base_datos):
    conexion = pika.BlockingConnection(parametros)
    try:
        canal = conexion.channel()
        declarar_topologia(canal)
        # Como máximo 1 mensaje sin confirmar a la vez para este consumidor.
        canal.basic_qos(prefetch_count=1)
        canal.basic_consume(
            queue=COLA_PYTHON,
            on_message_callback=crear_procesador(base_datos),
            auto_ack=False,
        )
        log.info("Conectado. Esperando pedidos en la cola '%s'. Ctrl+C para salir.", COLA_PYTHON)
        canal.start_consuming()
    finally:
        if conexion.is_open:
            conexion.close()


def main():
    parametros = cargar_configuracion()
    base_datos = abrir_base_datos()
    log.info("Base de datos: %s", RUTA_BASE_DATOS)

    try:
        while True:
            try:
                consumir(parametros, base_datos)
                # start_consuming solo termina si llamamos a stop_consuming (error de SQLite).
                log.error("Consumo detenido por un error de base de datos. Revisa pedidos.db.")
                sys.exit(1)
            except ProbableAuthenticationError:
                log.error("RabbitMQ rechazó el usuario o la contraseña. Revisa el archivo .env.")
                sys.exit(1)
            except AMQPChannelError as error:
                # Por ejemplo PRECONDITION_FAILED: la cola existe con otras propiedades.
                log.error("RabbitMQ rechazó una operación: %s", error)
                sys.exit(1)
            except AMQPConnectionError:
                log.warning(
                    "Sin conexión con RabbitMQ (%s:%s). Reintentando en %d s... "
                    "(¿está en marcha? docker compose ps)",
                    parametros.host, parametros.port, SEGUNDOS_ENTRE_REINTENTOS,
                )
                time.sleep(SEGUNDOS_ENTRE_REINTENTOS)
    except KeyboardInterrupt:
        log.info("Suscriptor detenido por el usuario.")
    finally:
        base_datos.close()


if __name__ == "__main__":
    main()
