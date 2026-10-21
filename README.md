# Sistema de pedidos con RabbitMQ

Proyecto académico de mensajería asíncrona con tres programas independientes
que se comunican a través de RabbitMQ:

| Programa | Lenguaje | Función |
|---|---|---|
| `publicador-java` | Java (Swing + Maven) | Formulario que publica pedidos en JSON |
| `suscriptor-python` | Python (pika + sqlite3) | Guarda cada pedido en una base de datos SQLite |
| `suscriptor-javascript` | Node.js (amqplib) | Agrega cada pedido al archivo `pedidos.txt` |

## Arquitectura

El publicador envía cada pedido al exchange `pedidos_exchange` (tipo fanout),
que entrega una copia a las colas `pedidos_python` y `pedidos_javascript`.
Cada suscriptor consume de su propia cola.

## Estado

En construcción. Las instrucciones de instalación y ejecución se completarán
en las siguientes etapas.