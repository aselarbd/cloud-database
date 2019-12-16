package de.tum.i13.kvtp2;

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

public class KVTP2Server {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private Selector selector;
    private final Map<String, BiConsumer<MessageWriter, Message>> handlers;
    private BiConsumer<MessageWriter, Message> defaultHandler;

    private TCPServerConnection serverConnection;

    private boolean shutdown = false;

    private Decoder decoder = new Base64Decoder();
    private Encoder encoder = new Base64Encoder();

    public KVTP2Server() throws IOException {
        this(SelectorProvider.provider());
    }

    public KVTP2Server(SelectorProvider provider) throws IOException {
        this.handlers = new HashMap<>();
        this.selector = provider.openSelector();
    }

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

    private void serve(StringWriter w, byte[] request) {
        String in = new String(request, ENCODING).trim(); // TODO: Maybe trim manually, might be faster
        String[] msgs = in.split("\\R");
        for (String s : msgs) {
            byte[] decodedRequest = decoder.decode(s.getBytes(ENCODING));
            Message msg = null;
            try {
                msg = Message.parse(new String(decodedRequest, ENCODING));
            } catch (MalformedMessageException e) {
                Message error = new Message("_error");
                error.put("msg", "malformed message");
                error.put("original", new String(decodedRequest, ENCODING));
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
            handlers.get(command).accept(writer, request);
        } else if (defaultHandler != null) {
            defaultHandler.accept(writer, request);
        }
        // neither correct handler nor default handler set -> drop request
    }

    public void setDefaultHandler(BiConsumer<MessageWriter, Message> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    /**
     * handle configures a new handler for this server. The given handler
     * will be called for incoming messages with the given command. Think of
     * the command as what route and methods are in http.
     *
     * @param command command to handle
     * @param handler handler to call for incoming message with command.
     */
    public void handle(String command, BiConsumer<MessageWriter, Message> handler) {
        this.handlers.put(command, handler);
    }

    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    public void shutdown() throws IOException {
        serverConnection.close();
        shutdown = true;
    }
}
