package de.tum.i13.kvtp2;

import de.tum.i13.kvtp2.middleware.Handler;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A KVTP2Server runs a nio-server, which can handle incoming messasges
 *
 * Handlers can be registered individually per server instance using {@link Handler}-implementations.
 */
public class KVTP2Server {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private Selector selector;
    private final Map<String, Handler> handlers;
    private Handler defaultHandler;

    private TCPServerConnection serverConnection;

    private boolean shutdown = false;

    private Decoder decoder = new Base64Decoder();
    private Encoder encoder = new Base64Encoder();

    /**
     * Create a new server with a default SelectorProvider
     * @throws IOException If an I/O error occurs
     */
    public KVTP2Server() throws IOException {
        this(SelectorProvider.provider());
    }

    /**
     * Creates a new server using the given SelectorProvider
     *
     * @param provider provider to use for the nio server
     * @throws IOException If an I/O error occurs
     */
    public KVTP2Server(SelectorProvider provider) throws IOException {
        this.handlers = new HashMap<>();
        this.selector = provider.openSelector();
    }

    /**
     * Start the server at the given address and port
     * @param address address to run this server on
     * @param port port to listen on. If 0, a random free port is selected.
     * @throws IOException If an I/O error occurs
     */
    public void start(String address, int port) throws IOException {
        listenTCP(address, port);
        while (!shutdown) {
            for (ChangeRequest cr : serverConnection.getPendingChanges()) {
                if (cr.selectionKey != null) {
                    cr.selectionKey.interestOps(cr.ops);
                } else {
                    cr.connection.register(selector, cr.ops);
                }
            }
            this.selector.select();
            serve();
        }
    }

    private void listenTCP(String address, int port) throws IOException {
        serverConnection = new TCPServerConnection(address, port, this.selector, this::serve);
    }

    /**
     * Get the port number on which this server is running
     *
     * @return local port number of this server.
     */
    public int getLocalPort() {
        return serverConnection.getLocalPort();
    }

    private void serve() throws IOException {
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while(iterator.hasNext()) {
            SelectionKey key = iterator.next();
            Connection c = (Connection) key.attachment();

            if (!key.isValid()) {
                continue;
            }

            if (key.isAcceptable()) {
                c.accept();
            }
            if (key.isConnectable()) {
                c.connect();
            }
            if (key.isReadable()) {
                c.read();
            }
            if (key.isValid() && key.isWritable()) {
                c.write();
            }
            iterator.remove();
        }
    }

    private void serve(StringWriter w, TCPMessage request) {
        String in = new String(request.getBytes(), ENCODING).trim(); // TODO: Maybe trim manually, might be faster
        String[] msgs = in.split("\\R");
        for (String s : msgs) {
            String decodedRequest = decoder.decode(s, ENCODING);
            Message msg = null;
            try {
                msg = Message.parse(decodedRequest);
                msg.setSrc(request.getRemoteAddress());
            } catch (MalformedMessageException e) {
                Message error = new Message("_error");
                error.put("msg", "malformed message");
                error.put("original", decodedRequest);
                serve(w, error);
                return;
            }
            serve(w, msg);
        }
    }

    /**
     * Helper server to serve requests as messages instead of byte arrays
     *
     * @param responseWriter Writer wo write a response
     * @param request the request to handle
     */
    public void serve(StringWriter responseWriter, Message request) {
        String command = request.getCommand();
        MessageWriter writer = new EncodedMessageWriter(responseWriter, encoder, ENCODING) {
            @Override
            public void write(Message message) {
                super.write(message);
            }
        };

        if (handlers.containsKey(command)) {
            handlers.get(command).handle(writer, request);
        } else if (defaultHandler != null) {
            defaultHandler.handle(writer, request);
        }
        // neither correct handler nor default handler set -> drop request
    }

    /**
     * Set a default handler, which will be used for all Messages, which are not
     * handled by another more specific handler.
     * @param defaultHandler defaultHandler to use
     */
    public void setDefaultHandler(Handler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    /**
     * handle configures a new handler for this server. The given handler
     * will be called for incoming messages with the given command. Think of
     * the command as a combination of what route and methods are in http.
     *
     * @param command command to handle
     * @param handler handler to call for incoming message with command.
     */
    public void handle(String command, Handler handler) {
        this.handlers.put(command, handler);
    }

    /**
     * Set a different decoder for this server (Default is base64)
     *
     * @param decoder decoder to use
     */
    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    /**
     * Set a different encoder for this server (Default is base64)
     *
     * @param encoder encoder to use
     */
    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    /**
     * Shutdown the server
     *
     * @throws IOException If an I/O error occurs
     */
    public void shutdown() throws IOException {
        serverConnection.close();
        shutdown = true;
    }
}
