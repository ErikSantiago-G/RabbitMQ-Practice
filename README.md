# Sistema de pedidos con RabbitMQ

Proyecto académico de **mensajería asíncrona**: tres programas independientes, escritos en
tres lenguajes distintos, que se comunican únicamente a través de RabbitMQ.

| Programa | Lenguaje | Función |
|---|---|---|
| `publicador-java` | Java 21 (Swing + Maven) | Formulario que valida y publica pedidos en JSON |
| `suscriptor-python` | Python 3 (pika + sqlite3) | Guarda cada pedido en la base de datos SQLite `pedidos.db` |
| `suscriptor-javascript` | Node.js (amqplib) | Agrega cada pedido al final del archivo `pedidos.txt` |

## Arquitectura

```
                       ┌──────────────── RabbitMQ (Docker) ────────────────┐
┌──────────────────┐   │                         ┌────────────────────┐    │   ┌─────────────────────┐
│ Publicador Java  │──▶│  pedidos_exchange ────▶ │ pedidos_python     │────┼──▶│ Suscriptor Python   │──▶ pedidos.db
│ (Swing)          │   │  (fanout, durable) ───▶ │ pedidos_javascript │────┼──▶│ Suscriptor Node.js  │──▶ pedidos.txt
└──────────────────┘   │                         └────────────────────┘    │   └─────────────────────┘
                       └───────────────────────────────────────────────────┘
```

- El publicador envía cada pedido al exchange **`pedidos_exchange`**, de tipo **fanout**.
- El fanout copia cada mensaje a **todas** las colas enlazadas: `pedidos_python` y `pedidos_javascript`.
- Cada suscriptor consume de **su propia cola**, así ambos reciben **todos** los pedidos.
  Si compartieran una sola cola, RabbitMQ repartiría los mensajes entre ellos
  (*competing consumers*) y cada pedido llegaría solo a uno.

Formato del mensaje (contrato entre los tres programas):

```json
{"producto": "camiseta", "cantidad": 2}
```

### Fiabilidad

| Mecanismo | Dónde | Qué evita |
|---|---|---|
| Exchange y colas **durables** | Los tres programas | Que la topología desaparezca al reiniciar RabbitMQ |
| Mensajes **persistentes** (`deliveryMode 2`) + volumen Docker | Publicador / `docker-compose.yml` | Que los mensajes en cola se pierdan al reiniciar RabbitMQ |
| **Publisher confirms** | Publicador | Informar "enviado" sin que RabbitMQ lo haya aceptado |
| Flag **mandatory** | Publicador | Que un pedido se descarte en silencio si no hay colas enlazadas |
| **ACK manual** después de guardar | Suscriptores | Perder un pedido si el suscriptor falla mientras lo procesa |
| **NACK sin reencolar** para mensajes inválidos | Suscriptores | Bucles infinitos con mensajes que nunca podrán procesarse |
| `mensaje_id` **UNIQUE** en SQLite | Suscriptor Python | Guardar dos veces un mensaje entregado dos veces |
| Reintento de conexión cada 5 s | Suscriptores (y publicador en cada envío) | Que una caída temporal de RabbitMQ obligue a reiniciar los programas |

**Límites:** el sistema ofrece entrega *al menos una vez* (*at-least-once*). Puede haber
duplicados (por ejemplo, si un suscriptor cae justo después de guardar y antes del ACK).
El suscriptor Python los descarta gracias a `mensaje_id`; el suscriptor Node.js podría escribir
una línea repetida. Además, los mensajes inválidos se descartan (no se usa una *dead letter queue*).

## Requisitos

| Herramienta | Versión probada | Comprobar con |
|---|---|---|
| Docker Desktop (con Docker Compose v2) | 28.x | `docker compose version` |
| Java JDK | 21 | `java -version` y `javac -version` |
| Maven | 3.9 o superior | `mvn -v` |
| Python | 3.11 o superior | `python --version` |
| Node.js | 20.12 o superior (probado con 22) | `node -v` |

## Instalación

Todos los comandos son para **PowerShell** en Windows.

```powershell
git clone https://github.com/ErikSantiago-G/<nombre-del-repositorio>.git
cd <nombre-del-repositorio>

# 1. Configuración (usuario y contraseña de RabbitMQ)
Copy-Item .env.example .env
# Edita .env y cambia RABBITMQ_PASSWORD

# 2. RabbitMQ
docker compose up -d
docker compose ps            # espera a que aparezca (healthy)

# 3. Suscriptor Python
cd suscriptor-python
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cd ..

# 4. Suscriptor Node.js
cd suscriptor-javascript
npm install
cd ..
```

## Ejecución

Usa **tres terminales**, una por programa. Inicia primero los suscriptores: al arrancar
crean sus colas y las enlazan al exchange.

```powershell
# Terminal 1: suscriptor Python
cd suscriptor-python
.\.venv\Scripts\Activate.ps1
python suscriptor.py

# Terminal 2: suscriptor Node.js
cd suscriptor-javascript
npm start

# Terminal 3: publicador con interfaz gráfica
cd publicador-java
mvn -q compile exec:java
```

Publicador de consola (envía 3 pedidos de prueba, sin interfaz):

```powershell
cd publicador-java
mvn -q compile exec:java "-Dexec.mainClass=com.pedidos.PublicadorPrueba"
```

## Verificación

| Qué comprobar | Cómo |
|---|---|
| Pedidos en SQLite | `cd suscriptor-python` y `python ver_pedidos.py` |
| Pedidos en el TXT | `Get-Content suscriptor-javascript\pedidos.txt -Encoding UTF8` |
| Colas, mensajes y bindings | Panel web http://localhost:15672 (usuario y contraseña del `.env`) |
| Colas desde la terminal | `docker exec rabbitmq-pedidos rabbitmqctl list_queues name messages durable` |
| Bindings desde la terminal | `docker exec rabbitmq-pedidos rabbitmqctl list_bindings source_name destination_name` |

## Pruebas realizadas

| # | Prueba | Resultado esperado |
|---|---|---|
| 1 | Enviar un pedido válido desde el formulario | Aparece en `pedidos.db` y en `pedidos.txt` |
| 2 | Cantidad vacía, `0`, `-3`, `2.5`, `dos` o producto vacío | El formulario muestra un error y no publica |
| 3 | Detener el suscriptor Node.js, enviar 3 pedidos y volver a iniciarlo | La cola `pedidos_javascript` acumula 3 mensajes y se procesan al iniciar |
| 4 | Detener los suscriptores, enviar pedidos y ejecutar `docker compose restart` | Los mensajes siguen en las colas tras el reinicio |
| 5 | Detener RabbitMQ con los suscriptores en marcha y volver a iniciarlo | Los suscriptores reintentan cada 5 s y se reconectan solos |
| 6 | Publicar texto que no es JSON desde el panel web | Los suscriptores lo descartan con un aviso y siguen funcionando |
| 7 | Contraseña incorrecta en `.env` | Mensaje "RabbitMQ rechazó el usuario o la contraseña" |
| 8 | Enviar un pedido con RabbitMQ detenido | El formulario muestra "No se pudo conectar con RabbitMQ" |

## Estructura

```
rabbitmq-pedidos/
├── docker-compose.yml          RabbitMQ 4 con panel de administración
├── .env.example                Plantilla de configuración (el .env real no se sube)
├── publicador-java/
│   ├── pom.xml
│   └── src/main/java/com/pedidos/
│       ├── VentanaPedidos.java       Formulario Swing (clase principal)
│       ├── ValidadorPedido.java      Validación del producto y la cantidad
│       ├── Pedido.java               Record con el contrato del mensaje
│       ├── PublicadorPedidos.java    Conexión, confirmaciones y publicación
│       ├── ConfiguracionRabbit.java  Lectura de .env y variables de entorno
│       ├── ErrorPublicacion.java     Errores con mensajes comprensibles
│       └── PublicadorPrueba.java     Publicador de consola para pruebas
├── suscriptor-python/
│   ├── requirements.txt
│   ├── suscriptor.py           Consume pedidos_python y guarda en SQLite
│   └── ver_pedidos.py          Muestra el contenido de pedidos.db
└── suscriptor-javascript/
    ├── package.json
    ├── package-lock.json
    └── suscriptor.js           Consume pedidos_javascript y escribe pedidos.txt
```

## Detener todo

```powershell
# Ctrl+C en las terminales de los suscriptores y cerrar la ventana Java
docker compose stop          # detiene RabbitMQ conservando los datos
docker compose down -v       # (opcional) borra RabbitMQ y todos sus datos
```
