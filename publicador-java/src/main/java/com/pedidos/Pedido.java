package com.pedidos;

/**
 * Representa un pedido que se envía a RabbitMQ.
 * Gson lo convierte en JSON con la forma {"producto":"camiseta","cantidad":2}.
 *
 * Los nombres "producto" y "cantidad" son el CONTRATO del mensaje:
 * los suscriptores Python y JavaScript esperan exactamente esos campos.
 */
public record Pedido(String producto, int cantidad) {

    /**
     * Constructor compacto: se ejecuta cada vez que se crea un Pedido.
     * Garantiza que nunca exista un pedido inválido, aunque alguien
     * olvide usar ValidadorPedido.
     */
    public Pedido {
        if (producto == null || producto.isBlank()) {
            throw new IllegalArgumentException("El producto no puede estar vacío.");
        }
        if (cantidad <= 0) {
            throw new IllegalArgumentException("La cantidad debe ser mayor que cero.");
        }
        producto = producto.strip();
    }
}
