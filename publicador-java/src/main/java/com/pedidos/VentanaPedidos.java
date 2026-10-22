package com.pedidos;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

/**
 * Formulario Swing del publicador.
 *
 * Regla de oro de Swing: la interfaz solo se toca desde el hilo de eventos
 * (Event Dispatch Thread, EDT). Las operaciones lentas (conectar, publicar)
 * se hacen en un hilo de fondo con SwingWorker para que la ventana no se congele.
 */
public class VentanaPedidos extends JFrame {

    private static final Color COLOR_EXITO = new Color(0, 128, 0);
    private static final Color COLOR_ERROR = new Color(178, 34, 34);
    private static final Color COLOR_INFO = Color.DARK_GRAY;
    private static final DateTimeFormatter FORMATO_HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final PublicadorPedidos publicador;

    private final JTextField campoProducto = new JTextField(20);
    private final JTextField campoCantidad = new JTextField(6);
    private final JButton botonEnviar = new JButton("Enviar pedido");
    private final JLabel etiquetaEstado = new JLabel(" ");
    private final DefaultListModel<String> historial = new DefaultListModel<>();

    public VentanaPedidos(PublicadorPedidos publicador) {
        super("Publicador de pedidos - RabbitMQ");
        this.publicador = publicador;

        construirInterfaz();

        botonEnviar.addActionListener(evento -> enviarPedido());
        getRootPane().setDefaultButton(botonEnviar); // Enter = enviar

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                publicador.close(); // cierra la conexión con RabbitMQ antes de salir
            }
        });

        pack();
        setMinimumSize(getSize());
        setLocationRelativeTo(null); // centrar en pantalla

        conectarEnSegundoPlano();
    }

    private void construirInterfaz() {
        JPanel formulario = new JPanel(new GridBagLayout());
        formulario.setBorder(BorderFactory.createTitledBorder("Nuevo pedido"));

        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(6, 8, 6, 8);
        c.anchor = GridBagConstraints.WEST;

        c.gridx = 0;
        c.gridy = 0;
        formulario.add(new JLabel("Producto:"), c);
        c.gridx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        formulario.add(campoProducto, c);

        c.gridx = 0;
        c.gridy = 1;
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        formulario.add(new JLabel("Cantidad:"), c);
        c.gridx = 1;
        formulario.add(campoCantidad, c);

        c.gridx = 1;
        c.gridy = 2;
        c.anchor = GridBagConstraints.EAST;
        formulario.add(botonEnviar, c);

        JList<String> listaHistorial = new JList<>(historial);
        JScrollPane panelHistorial = new JScrollPane(listaHistorial);
        panelHistorial.setBorder(BorderFactory.createTitledBorder("Pedidos enviados"));
        panelHistorial.setPreferredSize(new Dimension(420, 160));

        etiquetaEstado.setBorder(BorderFactory.createEmptyBorder(6, 10, 8, 10));

        JPanel contenido = new JPanel(new BorderLayout(0, 8));
        contenido.setBorder(BorderFactory.createEmptyBorder(10, 10, 0, 10));
        contenido.add(formulario, BorderLayout.NORTH);
        contenido.add(panelHistorial, BorderLayout.CENTER);

        getContentPane().setLayout(new BorderLayout());
        getContentPane().add(contenido, BorderLayout.CENTER);
        getContentPane().add(etiquetaEstado, BorderLayout.SOUTH);
    }

    /** Se ejecuta en el EDT al pulsar el botón o Enter. */
    private void enviarPedido() {
        Pedido pedido;
        try {
            pedido = ValidadorPedido.validar(campoProducto.getText(), campoCantidad.getText());
        } catch (IllegalArgumentException e) {
            mostrarEstado(e.getMessage(), COLOR_ERROR);
            if (e.getMessage().toLowerCase().contains("producto")) {
                campoProducto.requestFocusInWindow();
            } else {
                campoCantidad.requestFocusInWindow();
            }
            return;
        }

        botonEnviar.setEnabled(false); // evita envíos dobles mientras se publica
        mostrarEstado("Enviando pedido...", COLOR_INFO);

        new SwingWorker<String, Void>() {
            @Override
            protected String doInBackground() throws ErrorPublicacion {
                return publicador.publicar(pedido); // hilo de fondo: puede tardar
            }

            @Override
            protected void done() { // de vuelta en el EDT
                try {
                    String json = get();
                    historial.add(0, LocalTime.now().format(FORMATO_HORA) + "  " + json);
                    mostrarEstado("Pedido enviado y confirmado por RabbitMQ.", COLOR_EXITO);
                    campoProducto.setText("");
                    campoCantidad.setText("");
                    campoProducto.requestFocusInWindow();
                } catch (ExecutionException e) {
                    mostrarEstado(e.getCause().getMessage(), COLOR_ERROR);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    botonEnviar.setEnabled(true);
                }
            }
        }.execute();
    }

    private void conectarEnSegundoPlano() {
        mostrarEstado("Conectando a RabbitMQ...", COLOR_INFO);
        new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws ErrorPublicacion {
                publicador.conectar();
                return null;
            }

            @Override
            protected void done() {
                try {
                    get();
                    mostrarEstado("Conectado a " + publicador.describirConexion(), COLOR_EXITO);
                } catch (ExecutionException e) {
                    mostrarEstado(e.getCause().getMessage() + " Se reintentará al enviar.", COLOR_ERROR);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }.execute();
    }

    private void mostrarEstado(String texto, Color color) {
        // HTML permite que los mensajes largos ocupen varias líneas.
        etiquetaEstado.setText("<html><body style='width:380px'>" + escaparHtml(texto) + "</body></html>");
        etiquetaEstado.setForeground(color);
        pack();
    }

    private static String escaparHtml(String texto) {
        return texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (Exception ignorado) {
                // Si falla, Swing usa su aspecto por defecto.
            }

            ConfiguracionRabbit configuracion;
            try {
                configuracion = ConfiguracionRabbit.cargar();
            } catch (IllegalStateException e) {
                JOptionPane.showMessageDialog(null, e.getMessage(),
                        "Error de configuración", JOptionPane.ERROR_MESSAGE);
                System.exit(1);
                return;
            }

            new VentanaPedidos(new PublicadorPedidos(configuracion)).setVisible(true);
        });
    }
}
