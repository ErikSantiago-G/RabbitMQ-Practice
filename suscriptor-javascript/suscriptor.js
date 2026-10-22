/**
 * Suscriptor 2 (Node.js): recibe pedidos de RabbitMQ y los agrega a pedidos.txt.
 *
 * Recorrido de cada mensaje:
 *   pedidos_exchange (fanout) -> cola pedidos_javascript -> este programa -> pedidos.txt
 *
 * Ejecutar desde la carpeta suscriptor-javascript:
 *   npm start        (equivale a: node suscriptor.js)
 * Detener con Ctrl+C.
 */

const fs = require('node:fs');
const path = require('node:path');
const amqp = require('amqplib');

// --- Nombres de la topología (deben coincidir con los otros programas) ---
const EXCHANGE_PEDIDOS = 'pedidos_exchange';
const COLA_JAVASCRIPT = 'pedidos_javascript';

const RUTA_ARCHIVO = path.join(__dirname, 'pedidos.txt');
const RUTAS_ARCHIVO_ENV = [path.join(process.cwd(), '.env'), path.join(__dirname, '..', '.env')];
const MS_ENTRE_REINTENTOS = 5000;

let conexionActual = null;
let cerrando = false;

// ----------------------------------------------------------------------
// Configuración
// ----------------------------------------------------------------------
function cargarConfiguracion() {
  // process.loadEnvFile (Node >= 20.12) NO sobrescribe variables ya definidas,
  // así que una variable de entorno tiene prioridad sobre el archivo .env.
  const rutaEnv = RUTAS_ARCHIVO_ENV.find((ruta) => fs.existsSync(ruta));
  if (rutaEnv) {
    process.loadEnvFile(rutaEnv);
  }

  const usuario = process.env.RABBITMQ_USER;
  const contrasena = process.env.RABBITMQ_PASSWORD;
  if (!usuario || !contrasena) {
    console.error('Faltan RABBITMQ_USER o RABBITMQ_PASSWORD. Revisa el archivo .env en la raíz del proyecto.');
    process.exit(1);
  }

  const puertoTexto = process.env.RABBITMQ_PORT || '5672';
  if (!/^\d+$/.test(puertoTexto)) {
    console.error(`RABBITMQ_PORT debe ser un número entero, pero vale: ${puertoTexto}`);
    process.exit(1);
  }

  // Usamos un objeto (y no una URL "amqp://usuario:clave@host") para no tener
  // que codificar caracteres especiales de la contraseña.
  return {
    protocol: 'amqp',
    hostname: process.env.RABBITMQ_HOST || 'localhost',
    port: Number(puertoTexto),
    username: usuario,
    password: contrasena,
    vhost: process.env.RABBITMQ_VHOST || '/',
    heartbeat: 60,
  };
}

// ----------------------------------------------------------------------
// Mensajes
// ----------------------------------------------------------------------
/** Convierte el contenido recibido en {producto, cantidad} o lanza un Error. */
function interpretarMensaje(contenido) {
  let datos;
  try {
    datos = JSON.parse(contenido.toString('utf8'));
  } catch (error) {
    throw new Error(`no es JSON válido (${error.message})`);
  }
  if (datos === null || typeof datos !== 'object' || Array.isArray(datos)) {
    throw new Error('el JSON debe ser un objeto {...}');
  }

  const { producto, cantidad } = datos;
  if (typeof producto !== 'string' || producto.trim() === '') {
    throw new Error("falta 'producto' o no es un texto");
  }
  if (!Number.isInteger(cantidad) || cantidad <= 0) {
    throw new Error("'cantidad' debe ser un entero mayor que cero");
  }
  return { producto: producto.trim(), cantidad };
}

function fechaLocal() {
  const ahora = new Date();
  const dosDigitos = (n) => String(n).padStart(2, '0');
  return (
    `${ahora.getFullYear()}-${dosDigitos(ahora.getMonth() + 1)}-${dosDigitos(ahora.getDate())} ` +
    `${dosDigitos(ahora.getHours())}:${dosDigitos(ahora.getMinutes())}:${dosDigitos(ahora.getSeconds())}`
  );
}

async function guardarPedido({ producto, cantidad }) {
  const linea = `${fechaLocal()} | producto: ${producto} | cantidad: ${cantidad}\n`;
  // appendFile abre el archivo en modo "a" (append): agrega al final y
  // conserva lo anterior. Si el archivo no existe, lo crea.
  await fs.promises.appendFile(RUTA_ARCHIVO, linea, 'utf8');
}

function crearProcesador(canal) {
  return async (mensaje) => {
    if (mensaje === null) {
      // RabbitMQ canceló el consumidor (por ejemplo, borraron la cola).
      console.warn('El consumidor fue cancelado por RabbitMQ.');
      return;
    }

    let pedido;
    try {
      pedido = interpretarMensaje(mensaje.content);
    } catch (error) {
      console.warn(`Mensaje inválido descartado: ${error.message}. Contenido: ${mensaje.content.toString('utf8').slice(0, 200)}`);
      canal.nack(mensaje, false, false); // NACK sin reencolar
      return;
    }

    try {
      await guardarPedido(pedido);
    } catch (error) {
      console.error(`No se pudo escribir en pedidos.txt: ${error.message}. El mensaje vuelve a la cola.`);
      canal.nack(mensaje, false, true); // reencolar: no perdemos el pedido
      await detener(1);
      return;
    }

    // ACK solo DESPUÉS de escribir en el archivo.
    canal.ack(mensaje);
    console.log(`Pedido guardado: ${pedido.producto} x ${pedido.cantidad}`);
  };
}

// ----------------------------------------------------------------------
// Conexión
// ----------------------------------------------------------------------
async function iniciar(configuracion) {
  try {
    const conexion = await amqp.connect(configuracion);
    conexionActual = conexion;

    conexion.on('error', (error) => {
      console.error(`Error en la conexión con RabbitMQ: ${error.message}`);
    });
    conexion.on('close', () => {
      conexionActual = null;
      if (!cerrando) {
        console.warn(`Se perdió la conexión con RabbitMQ. Reintentando en ${MS_ENTRE_REINTENTOS / 1000} s...`);
        setTimeout(() => iniciar(configuracion), MS_ENTRE_REINTENTOS);
      }
    });

    const canal = await conexion.createChannel();
    // Idempotente: crea exchange, cola y binding si no existen.
    await canal.assertExchange(EXCHANGE_PEDIDOS, 'fanout', { durable: true });
    await canal.assertQueue(COLA_JAVASCRIPT, { durable: true });
    await canal.bindQueue(COLA_JAVASCRIPT, EXCHANGE_PEDIDOS, '');
    await canal.prefetch(1); // un mensaje sin confirmar a la vez

    await canal.consume(COLA_JAVASCRIPT, crearProcesador(canal), { noAck: false });
    console.log(`Conectado. Esperando pedidos en la cola '${COLA_JAVASCRIPT}'. Archivo: ${RUTA_ARCHIVO}`);
    console.log('Ctrl+C para salir.');
  } catch (error) {
    if (/ACCESS_REFUSED|403/.test(error.message)) {
      console.error('RabbitMQ rechazó el usuario o la contraseña. Revisa el archivo .env.');
      process.exit(1);
    }
    if (/PRECONDITION_FAILED/.test(error.message)) {
      console.error(`RabbitMQ rechazó la declaración: ${error.message}`);
      process.exit(1);
    }
    console.warn(
      `Sin conexión con RabbitMQ (${configuracion.hostname}:${configuracion.port}): ${error.code || error.message}. ` +
        `Reintentando en ${MS_ENTRE_REINTENTOS / 1000} s... (¿está en marcha? docker compose ps)`,
    );
    setTimeout(() => iniciar(configuracion), MS_ENTRE_REINTENTOS);
  }
}

async function detener(codigoSalida = 0) {
  cerrando = true;
  try {
    if (conexionActual) {
      await conexionActual.close();
    }
  } catch {
    // La conexión ya estaba cerrada.
  }
  process.exit(codigoSalida);
}

process.on('SIGINT', () => {
  console.log('\nSuscriptor detenido por el usuario.');
  detener(0);
});

iniciar(cargarConfiguracion());
