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

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private Selector selector;

    private Encoder encoder = new Base64Encoder();
    private Decoder decoder = new Base64Decoder();

    private Map<InetSocketAddress, Connection> connections = new HashMap<>();
    private Map<Integer, BiConsumer<MessageWriter, Message>> handlers = new HashMap<>();

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
            if (key.isWritable()) {
                c.write();
            }
            iterator.remove();
        }
    }

    public Future<Boolean> connect(String address, int port) throws IOException {
        InetSocketAddress isa = new InetSocketAddress(address, port);
        return connect(isa);
    }

    private Future<Boolean> connect(InetSocketAddress address) throws IOException {
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(false);
        sc.connect(address);
        TCPConnection connection = new TCPConnection(sc, this::receive);
        this.connections.put(address, connection);
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

    public void send(Message m, BiConsumer<MessageWriter, Message> r) throws IOException, ExecutionException, InterruptedException {
        // TODO: find the correct connection or create a new connection, encode and send the data and store the response handler
        InetSocketAddress target = new InetSocketAddress(m.get("host"), Integer.parseInt(m.get("port")));

        if (!connections.containsKey(target)) {
            connect(target).get();
        }
        this.handlers.put(m.getID(), r);
        Connection connection = connections.get(target);
        StringWriter stringWriter = connection.getStringWriter();
        stringWriter.write(encoder.encode(m.toString()));
        stringWriter.flush();
    }

    private void receive(StringWriter w, byte[] request) {
        String in = new String(request, ENCODING).trim(); // TODO: Maybe trim manually, might be faster
        byte[] decodedRequest = decoder.decode(in.getBytes(ENCODING));
        Message msg = Message.parse(new String(decodedRequest, ENCODING));
        receive(w, msg);
    }

    private void receive(StringWriter w, Message m) {
        if (handlers.containsKey(m.getID())) {
            EncodedMessageWriter encodedMessageWriter = new EncodedMessageWriter(w, encoder, ENCODING);
            handlers.get(m.getID()).accept(encodedMessageWriter, m);
        }
        // drop message
        // TODO: default handler?
    }
}
