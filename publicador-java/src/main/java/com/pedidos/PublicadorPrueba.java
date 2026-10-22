package com.pedidos;

import java.util.List;

/**
 * Publicador de consola, sin interfaz gráfica.
 * Sirve para probar el sistema rápidamente (Etapas 3 y 6).
 *
 * Ejecutar desde la carpeta publicador-java:
 *   mvn -q compile exec:java "-Dexec.mainClass=com.pedidos.PublicadorPrueba"
 */
public class PublicadorPrueba {

    public static void main(String[] args) {
        List<Pedido> pedidos = List.of(
                new Pedido("camiseta", 2),
                new Pedido("pantalón", 1),
                new Pedido("pañuelo \"edición especial\"", 5)
        );

        ConfiguracionRabbit configuracion;
        try {
            configuracion = ConfiguracionRabbit.cargar();
        } catch (IllegalStateException e) {
            System.err.println("Error de configuración: " + e.getMessage());
            return;
        }

        // try-with-resources: cierra la conexión al terminar, aunque haya errores.
        try (PublicadorPedidos publicador = new PublicadorPedidos(configuracion)) {
            System.out.println("Conectando a RabbitMQ en " + publicador.describirConexion() + "...");
            publicador.conectar();
            for (Pedido pedido : pedidos) {
                String json = publicador.publicar(pedido);
                System.out.println("Pedido confirmado por RabbitMQ: " + json);
            }
        } catch (ErrorPublicacion e) {
            System.err.println(e.getMessage());
        }
    }
}
