package com.pedidos;

/**
 * Convierte el texto escrito en el formulario en un Pedido válido.
 * Está separado de la ventana para poder probarlo sin interfaz gráfica.
 */
public final class ValidadorPedido {

    public static final int LONGITUD_MAXIMA_PRODUCTO = 100;
    public static final int CANTIDAD_MAXIMA = 10_000;

    private ValidadorPedido() {
        // Clase de utilidades: no se crean objetos de ella.
    }

    /**
     * @return un Pedido válido.
     * @throws IllegalArgumentException con un mensaje pensado para el usuario.
     */
    public static Pedido validar(String productoTexto, String cantidadTexto) {
        String producto = productoTexto == null ? "" : productoTexto.strip();
        String cantidad = cantidadTexto == null ? "" : cantidadTexto.strip();

        if (producto.isEmpty()) {
            throw new IllegalArgumentException("Escribe el nombre del producto.");
        }
        if (producto.length() > LONGITUD_MAXIMA_PRODUCTO) {
            throw new IllegalArgumentException(
                    "El producto no puede tener más de " + LONGITUD_MAXIMA_PRODUCTO + " caracteres.");
        }
        if (cantidad.isEmpty()) {
            throw new IllegalArgumentException("Escribe la cantidad.");
        }
        // Solo dígitos: rechaza "2.5", "-3", "+3", "2e3", "dos" y espacios internos.
        if (!cantidad.matches("\\d+")) {
            throw new IllegalArgumentException(
                    "La cantidad debe ser un número entero positivo (sin decimales ni signos).");
        }
        // Evita desbordar int con números enormes antes de convertir.
        if (cantidad.length() > 9) {
            throw new IllegalArgumentException("La cantidad no puede ser mayor que " + CANTIDAD_MAXIMA + ".");
        }

        int valor = Integer.parseInt(cantidad);
        if (valor <= 0) {
            throw new IllegalArgumentException("La cantidad debe ser mayor que cero.");
        }
        if (valor > CANTIDAD_MAXIMA) {
            throw new IllegalArgumentException("La cantidad no puede ser mayor que " + CANTIDAD_MAXIMA + ".");
        }
        return new Pedido(producto, valor);
    }
}
