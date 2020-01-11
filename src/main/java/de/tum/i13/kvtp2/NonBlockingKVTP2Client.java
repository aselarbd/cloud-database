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

public class NonBlockingKVTP2Client {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;

    private Selector selector;

    private Encoder encoder = new Base64Encoder();
    private Decoder decoder = new Base64Decoder();

    private final Map<InetSocketAddress, Connection> connections = new HashMap<>();
    private final Map<Integer, BiConsumer<MessageWriter, Message>> handlers = new HashMap<>();
    private BiConsumer<MessageWriter, Message> defaultHandler;

    private InetSocketAddress defaultConnection;

    public NonBlockingKVTP2Client() throws IOException {
        this(SelectorProvider.provider());
    }

    public NonBlockingKVTP2Client(SelectorProvider provider) throws IOException {
        this.selector = provider.openSelector();
    }

    public void start() throws IOException {
        while(true) {
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

    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
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

    public Future<Boolean> connect(String address, int port) throws IOException {
        InetSocketAddress isa = new InetSocketAddress(address, port);
        return connect(isa);
    }

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

    public void send(Message m) {
        send(m, defaultHandler);
    }

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

    public void setDefaultHandler(BiConsumer<MessageWriter, Message> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }
}
