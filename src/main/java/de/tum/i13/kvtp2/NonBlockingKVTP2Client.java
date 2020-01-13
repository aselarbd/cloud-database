package de.tum.i13.kvtp2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * {@link NonBlockingKVTP2Client} provides a non blocking client, for the kvtp2 protocol.
 * The client is supposed to be run in background in a separate thread, operations can be executed
 * from the main thread.
 */
public class NonBlockingKVTP2Client {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;

    private Selector selector;

    private Encoder encoder = new Base64Encoder();
    private Decoder decoder = new Base64Decoder();

    private final Map<InetSocketAddress, Connection> connections = new HashMap<>();
    private final Map<Integer, BiConsumer<MessageWriter, Message>> handlers = new HashMap<>();
    private BiConsumer<MessageWriter, Message> defaultHandler;

    private InetSocketAddress defaultConnection;

    private boolean exit = false;

    /**
     * Create a new client with a default SelectorProvider
     * @throws IOException If an I/O error occurs
     */
    public NonBlockingKVTP2Client() throws IOException {
        this(SelectorProvider.provider());
    }

    /**
     * Creates a new client using the given SelectorProvider
     *
     * @param provider provider to use for the nio client
     * @throws IOException If an I/O error occurs
     */
    public NonBlockingKVTP2Client(SelectorProvider provider) throws IOException {
        this.selector = provider.openSelector();
    }

    /**
     * Start the client event loop
     *
     * @throws IOException If an I/O error occurs
     */
    public void start() throws IOException {
        while(!exit) {
            for (ChangeRequest cr : getPendingChanges()) {
                if (cr.selectionKey != null) {
                    cr.selectionKey.interestOps(cr.ops);
                } else {
                    cr.connection.register(selector, cr.ops);
                }
            }
            this.selector.select();
            handleSelect();
        }
    }

    /**
     * Set a different decoder for this client (Default is base64)
     *
     * @param decoder decoder to use
     */
    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    /**
     * Set a different encoder for this client (Default is base64)
     *
     * @param encoder encoder to use
     */
    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    public void quit() {
        exit = true;
        this.selector.wakeup();
    }

    private synchronized List<ChangeRequest> getPendingChanges() {
        List<ChangeRequest> crs = new LinkedList<>();
        connections.forEach((a, c) -> crs.addAll(c.getPendingChanges()));
        return crs;
    }

    private void handleSelect() throws IOException {
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            Connection c = (Connection) key.attachment();

            if (!key.isValid()) {
                continue;
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

    public boolean isConnected() {
        if (defaultConnection != null) {
            return connections.get(defaultConnection).channel.keyFor(this.selector).isValid();
        }
        return false;
    }

    /**
     * Connect to the server at address:port. Returns a future, which will resolve,
     * when the connection was established. This is supposed to be called from
     * the main thread.
     *
     * @param address address to connect to
     * @param port port to connect to
     * @return a Future object, which resolves when the operation is finished.
     * @throws IOException If an I/O error occurs
     */
    public Future<Boolean> connect(String address, int port) throws IOException {
        InetSocketAddress isa = new InetSocketAddress(address, port);
        return connect(isa);
    }

    /**
     * Connect to the server at address. Returns a future, which will resolve,
     * when the connection was established. This is supposed to be called from
     * the main thread.
     *
     * @param address address to connect to
     * @return a Future object, which resolves when the operation is finished.
     * @throws IOException If an I/O error occurs
     */
    public Future<Boolean> connect(InetSocketAddress address) throws IOException {
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(false);
        sc.connect(address);
        TCPConnection connection = new TCPConnection(sc, this::receive);
        this.connections.put(address, connection);
        if (connections.size() == 1 && defaultConnection == null) {
            defaultConnection = address;
        }
        return new Future<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return connection.key != null;
            }

            @Override
            public Boolean get() throws InterruptedException {
                while (!isDone()) {
                    Thread.sleep(500);
                }
                return true;
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException {
                Instant start = Instant.now();
                while (!isDone()) {
                    Thread.sleep(100);
                    Instant now = Instant.now();
                    Duration duration = Duration.between(start, now);
                    long convertDuration = unit.convert(duration);
                    if (convertDuration > timeout) {
                        return isDone();
                    }
                }
                return true;
            }
        };
    }

    /**
     * send an asynchronous message to the connected server. Any responses
     * will be handled by a defaultHandler if present or otherwise dropped.
     *
     * @param m message to send.
     */
    public void send(Message m) {
        send(m, defaultHandler);
    }

    /**
     * Send an asynchronous message to the server. Responses will be handled by
     * the given BiConsumer.
     *
     * @param m Message to send
     * @param r BiConsumer, which will handle any responses.
     */
    public void send(Message m, BiConsumer<MessageWriter, Message> r) {
        InetSocketAddress target = defaultConnection;

        if (m.get("host") != null && m.get("port") != null) {
            target = new InetSocketAddress(m.get("host"), Integer.parseInt(m.get("port")));
        }

        this.handlers.put(m.getID(), r);
        Connection connection = connections.get(target);
        EncodedMessageWriter wr = new EncodedMessageWriter(connection.getStringWriter(), encoder, ENCODING);
        wr.write(m);
        wr.flush();
    }

    /**
     * Same as {@link NonBlockingKVTP2Client#send(Message, BiConsumer)} but connects to a server first.
     *
     * @param target server to connect to
     * @param m Message to send
     * @param r BiConsumer, which will handle any responses.
     * @throws IOException If an I/O error occurs
     * @throws ExecutionException If the connection fails
     * @throws InterruptedException If the connection is interrupted
     */
    public void send(InetSocketAddress target, Message m, BiConsumer<MessageWriter, Message> r) throws IOException, ExecutionException, InterruptedException {
        if (!connections.containsKey(target)) {
            connect(target).get();
        }
        this.handlers.put(m.getID(), r);
        Connection connection = connections.get(target);
        EncodedMessageWriter wr = new EncodedMessageWriter(connection.getStringWriter(), encoder, ENCODING);
        wr.write(m);
        wr.flush();
    }

    private void receive(StringWriter w, TCPMessage request) {
        String in = new String(request.getBytes(), ENCODING).trim(); // TODO: Maybe trim manually, might be faster
        String[] msgs = in.split("\r\n");
        for (String s : msgs) {
            String decodedRequest = decoder.decode(s, ENCODING);
            Message msg = null;
            try {
                msg = Message.parse(decodedRequest);
            } catch (MalformedMessageException e) {
                Message error = new Message("_error");
                error.put("msg", "malformed message");
                error.put("original", decodedRequest);
                receive(w, error);
                return;
            }
            receive(w, msg);
        }
    }

    private void receive(StringWriter w, Message m) {
        EncodedMessageWriter encodedMessageWriter = new EncodedMessageWriter(w, encoder, ENCODING);
        if (handlers.containsKey(m.getID())) {
            handlers.get(m.getID()).accept(encodedMessageWriter, m);
        } else if (defaultHandler != null) {
            defaultHandler.accept(encodedMessageWriter, m);
        }
        // neither correct handler nor default handler set -> drop response
    }

    /**
     * Set a default handler for all response messages, which are not handled by
     * a more specific handler.
     *
     * @param defaultHandler Default BiConsumer to handle responses
     */
    public void setDefaultHandler(BiConsumer<MessageWriter, Message> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }
}
