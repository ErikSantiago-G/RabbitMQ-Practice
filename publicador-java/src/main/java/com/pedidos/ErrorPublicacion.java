package com.pedidos;

/**
 * Error al conectar o publicar, con un mensaje pensado para mostrarse
 * directamente al usuario (en la ventana o en la consola).
 */
public class ErrorPublicacion extends Exception {

    public ErrorPublicacion(String mensaje) {
        super(mensaje);
    }

    public ErrorPublicacion(String mensaje, Throwable causa) {
        super(mensaje, causa);
    }
}
