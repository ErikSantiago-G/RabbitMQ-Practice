package com.pedidos;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Publica pedidos en el exchange fanout "pedidos_exchange".
 *
 * Decisiones importantes:
 *  - Una sola conexión y un solo canal, reutilizados en cada publicación.
 *  - Los métodos son "synchronized" porque un canal no debe usarse desde
 *    varios hilos a la vez (la ventana publica desde hilos de fondo).
 *  - Confirmaciones de publicación (publisher confirms): esperamos a que
 *    RabbitMQ diga "lo tengo" antes de informar éxito al usuario.
 *  - mandatory = true: si el exchange no tiene colas enlazadas, RabbitMQ
 *    nos devuelve el mensaje en lugar de descartarlo en silencio.
 *  - Si la conexión se pierde, se vuelve a abrir en la siguiente publicación.
 */
public class PublicadorPedidos implements AutoCloseable {

    public static final String EXCHANGE_PEDIDOS = "pedidos_exchange";
    private static final String ROUTING_KEY = ""; // El fanout ignora la routing key.
    private static final boolean OBLIGATORIO_ENRUTAR = true; // flag "mandatory"
    private static final long ESPERA_CONFIRMACION_MS = 5_000;

    private final ConfiguracionRabbit configuracion;
    private final ConnectionFactory fabrica;
    private final Gson gson = new Gson();

    private Connection conexion;
    private Channel canal;
    private volatile boolean mensajeDevuelto;

    public PublicadorPedidos(ConfiguracionRabbit configuracion) {
        this.configuracion = configuracion;
        this.fabrica = configuracion.crearFabricaConexiones();
    }

    /** Abre la conexión por adelantado (opcional: publicar() también la abre). */
    public synchronized void conectar() throws ErrorPublicacion {
        try {
            asegurarConexion();
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            cerrarSilenciosamente();
            throw traducirError(e);
        }
    }

    /**
     * Publica el pedido y espera la confirmación de RabbitMQ.
     *
     * @return el JSON enviado (para mostrarlo en pantalla).
     */
    public synchronized String publicar(Pedido pedido) throws ErrorPublicacion {
        String json = gson.toJson(pedido);
        byte[] cuerpo = json.getBytes(StandardCharsets.UTF_8);

        AMQP.BasicProperties propiedades = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .contentEncoding("UTF-8")
                .deliveryMode(2)                           // 2 = persistente
                .messageId(UUID.randomUUID().toString())   // permite detectar duplicados
                .timestamp(new Date())
                .build();

        try {
            asegurarConexion();
            mensajeDevuelto = false;
            canal.basicPublish(EXCHANGE_PEDIDOS, ROUTING_KEY, OBLIGATORIO_ENRUTAR, propiedades, cuerpo);

            // Bloquea hasta que RabbitMQ confirme (ack) o rechace (nack) el mensaje.
            boolean confirmado = canal.waitForConfirms(ESPERA_CONFIRMACION_MS);
            if (!confirmado) {
                throw new ErrorPublicacion("RabbitMQ no pudo guardar el pedido (nack). Inténtalo de nuevo.");
            }
            // Si no había colas enlazadas, RabbitMQ devolvió el mensaje ANTES del ack.
            if (mensajeDevuelto) {
                throw new ErrorPublicacion("Ninguna cola está enlazada a '" + EXCHANGE_PEDIDOS
                        + "' y el pedido se descartó. Inicia los suscriptores al menos una vez "
                        + "para que creen sus colas.");
            }
            return json;

        } catch (TimeoutException e) {
            // No sabemos si RabbitMQ guardó el mensaje: cerramos el canal para empezar limpio.
            cerrarSilenciosamente();
            throw new ErrorPublicacion("RabbitMQ no confirmó el pedido a tiempo. "
                    + "Puede que se haya guardado o no: revisa los suscriptores antes de reenviarlo.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ErrorPublicacion("La publicación se interrumpió.", e);
        } catch (IOException | ShutdownSignalException e) {
            cerrarSilenciosamente();
            throw traducirError(e);
        }
    }

    @Override
    public synchronized void close() {
        cerrarSilenciosamente();
    }

    public String describirConexion() {
        return configuracion.describir();
    }

    // ------------------------------------------------------------------

    private void asegurarConexion() throws IOException, TimeoutException {
        if (canal != null && canal.isOpen()) {
            return;
        }
        cerrarSilenciosamente();
        conexion = fabrica.newConnection("publicador-java");
        canal = conexion.createChannel();
        // Idempotente: crea el exchange si no existe. durable = true.
        canal.exchangeDeclare(EXCHANGE_PEDIDOS, BuiltinExchangeType.FANOUT, true);
        canal.confirmSelect(); // activa las confirmaciones de publicación en este canal
        canal.addReturnListener(mensaje -> mensajeDevuelto = true);
    }

    private void cerrarSilenciosamente() {
        try {
            if (canal != null && canal.isOpen()) {
                canal.close();
            }
        } catch (Exception ignorado) {
            // Al cerrar no hay nada útil que hacer con un error.
        }
        try {
            if (conexion != null && conexion.isOpen()) {
                conexion.close();
            }
        } catch (Exception ignorado) {
            // Igual que arriba.
        }
        canal = null;
        conexion = null;
    }

    private ErrorPublicacion traducirError(Exception e) {
        Throwable causa = e;
        while (causa != null) {
            if (causa instanceof PossibleAuthenticationFailureException) {
                return new ErrorPublicacion("RabbitMQ rechazó el usuario o la contraseña. "
                        + "Revisa RABBITMQ_USER y RABBITMQ_PASSWORD en el archivo .env.", e);
            }
            if (causa instanceof ConnectException
                    || causa instanceof SocketTimeoutException
                    || causa instanceof UnknownHostException) {
                return new ErrorPublicacion("No se pudo conectar con RabbitMQ (" + configuracion.describir()
                        + "). ¿Está en marcha? Compruébalo con: docker compose ps", e);
            }
            causa = causa.getCause();
        }
        String detalle = e.getMessage();
        if ((detalle == null || detalle.isBlank()) && e.getCause() != null) {
            detalle = e.getCause().getMessage();
        }
        return new ErrorPublicacion("Error de comunicación con RabbitMQ: " + detalle, e);
    }
}
