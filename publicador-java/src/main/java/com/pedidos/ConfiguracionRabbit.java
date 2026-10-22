package com.pedidos;

import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lee la configuración de conexión a RabbitMQ.
 *
 * Orden de prioridad para cada valor:
 *   1. Variable de entorno (por ejemplo, RABBITMQ_USER).
 *   2. Archivo .env (en la carpeta actual o en la carpeta superior).
 *   3. Valor por defecto (solo para host, puerto y virtual host).
 */
public final class ConfiguracionRabbit {

    private static final List<Path> RUTAS_ARCHIVO_ENV = List.of(
            Path.of(".env"),
            Path.of("..", ".env")
    );

    private final String host;
    private final int puerto;
    private final String usuario;
    private final String contrasena;
    private final String virtualHost;

    private ConfiguracionRabbit(String host, int puerto, String usuario,
                                String contrasena, String virtualHost) {
        this.host = host;
        this.puerto = puerto;
        this.usuario = usuario;
        this.contrasena = contrasena;
        this.virtualHost = virtualHost;
    }

    public static ConfiguracionRabbit cargar() {
        Map<String, String> valoresArchivo = leerArchivoEnv();

        String host = obtenerValor("RABBITMQ_HOST", valoresArchivo, "localhost");
        String puertoTexto = obtenerValor("RABBITMQ_PORT", valoresArchivo, "5672");
        String usuario = obtenerValor("RABBITMQ_USER", valoresArchivo, null);
        String contrasena = obtenerValor("RABBITMQ_PASSWORD", valoresArchivo, null);
        String virtualHost = obtenerValor("RABBITMQ_VHOST", valoresArchivo, "/");

        if (usuario == null || contrasena == null) {
            throw new IllegalStateException(
                    "Faltan RABBITMQ_USER o RABBITMQ_PASSWORD. "
                    + "Revisa el archivo .env en la raíz del proyecto.");
        }

        int puerto;
        try {
            puerto = Integer.parseInt(puertoTexto);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "RABBITMQ_PORT debe ser un número entero, pero vale: " + puertoTexto);
        }

        return new ConfiguracionRabbit(host, puerto, usuario, contrasena, virtualHost);
    }

    public ConnectionFactory crearFabricaConexiones() {
        ConnectionFactory fabrica = new ConnectionFactory();
        fabrica.setHost(host);
        fabrica.setPort(puerto);
        fabrica.setUsername(usuario);
        fabrica.setPassword(contrasena);
        fabrica.setVirtualHost(virtualHost);
        // Si RabbitMQ no responde en 5 s, fallamos con un error claro
        // en lugar de dejar la ventana esperando.
        fabrica.setConnectionTimeout(5_000);
        // Desactivamos la recuperación automática del cliente: reconectamos
        // nosotros, de forma explícita, en PublicadorPedidos.asegurarConexion().
        fabrica.setAutomaticRecoveryEnabled(false);
        return fabrica;
    }

    /** Describe la conexión sin mostrar la contraseña. */
    public String describir() {
        return usuario + "@" + host + ":" + puerto + " (vhost " + virtualHost + ")";
    }

    private static String obtenerValor(String nombre, Map<String, String> valoresArchivo,
                                       String valorPorDefecto) {
        String valor = System.getenv(nombre);
        if (valor == null || valor.isBlank()) {
            valor = valoresArchivo.get(nombre);
        }
        if (valor == null || valor.isBlank()) {
            valor = valorPorDefecto;
        }
        return valor;
    }

    private static Map<String, String> leerArchivoEnv() {
        Map<String, String> valores = new HashMap<>();

        for (Path ruta : RUTAS_ARCHIVO_ENV) {
            if (!Files.isRegularFile(ruta)) {
                continue;
            }
            try {
                for (String linea : Files.readAllLines(ruta, StandardCharsets.UTF_8)) {
                    String lineaLimpia = linea.strip();
                    if (lineaLimpia.isEmpty() || lineaLimpia.startsWith("#")) {
                        continue;
                    }
                    int posicionIgual = lineaLimpia.indexOf('=');
                    if (posicionIgual <= 0) {
                        continue;
                    }
                    String clave = lineaLimpia.substring(0, posicionIgual).strip();
                    String valor = lineaLimpia.substring(posicionIgual + 1).strip();
                    valores.put(clave, valor);
                }
            } catch (IOException e) {
                throw new IllegalStateException(
                        "No se pudo leer el archivo " + ruta.toAbsolutePath() + ": " + e.getMessage(), e);
            }
            return valores; // Usamos solo el primer .env encontrado.
        }

        return valores;
    }
}
