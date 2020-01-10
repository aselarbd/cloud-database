package de.tum.i13.kvtp2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

class TCPConnection extends Connection {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private static final int BUFFER_SIZE = 8 * 1024;

    private final ByteBuffer readBuffer;

    private final BiConsumer<StringWriter, TCPMessage> receiver;

    private final SocketChannel channel;
    private final InetSocketAddress remoteAddress;

    private TCPConnStringWriter tcpConnStringWriter;
    private byte[] pendingRead;

    TCPConnection(SocketChannel channel, BiConsumer<StringWriter, TCPMessage> receiver) throws IOException {
        super(channel);
        this.receiver = receiver;
        this.channel = (SocketChannel) super.channel;

        this.remoteAddress = (InetSocketAddress) channel.getRemoteAddress();

        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);

        int ops = channel.isConnected() ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT;
        synchronized (pendingChanges) {
            this.pendingChanges.add(new ChangeRequest(this, ops));
        }
    }

    @Override
    void connect() throws IOException {
        channel.finishConnect();

        // only called from main client thread so this is fine
        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    }

    @Override
    void read() throws IOException {
        readBuffer.clear();
        int numRead = channel.read(readBuffer);
        if (numRead == -1) {
            channel.close();
            key.cancel();
            return;
        }

        byte[] data = new byte[numRead];

        System.arraycopy(readBuffer.array(), 0, data, 0, numRead);

        if (pendingRead != null && pendingRead.length > 0) {
            byte[] concatenated = new byte[pendingRead.length + data.length];
            System.arraycopy(pendingRead, 0, concatenated, 0, pendingRead.length);
            System.arraycopy(data, 0, concatenated, pendingRead.length, data.length);
            data = concatenated;
        }

        if (data.length > 128000) {
            channel.close();
            key.cancel();
            return;
        }

        if (receiver != null) {
            pendingRead = processReceiveBuffer(data);
        }
        // drop message
    }

    private byte[] processReceiveBuffer(byte[] data) {
        int length = data.length;
        int start = 0;
        for(int i = 1; i < length; i++) {
            if(data[i] == '\n') {
                if(data[i-1] == '\r') {

                    byte[] concatenated = new byte[(i-1) - start];
                    System.arraycopy(data, start, concatenated, 0, (i-1) - start);

                    TCPMessage tcpMessage = new TCPMessage(concatenated, this.remoteAddress);
                    receiver.accept(getStringWriter(), tcpMessage);

                    start = i + 1;
                }
            }
        }
        byte[] unprocessed = new byte[data.length - start];
        System.arraycopy(data, start, unprocessed, 0, unprocessed.length);

        return unprocessed;
    }

    public class TCPConnStringWriter implements StringWriter {
        @Override
        public void write(String string) {
            synchronized (pendingChanges) {
                synchronized (pendingWrites) {
                    pendingChanges.add(new ChangeRequest(key, key.interestOps() | SelectionKey.OP_WRITE));
                    pendingWrites.add(ByteBuffer.wrap((string + "\r\n").getBytes(ENCODING)));
                }
            }
        }

        @Override
        public void flush() {
            key.selector().wakeup();
        }

        @Override
        public void close() throws IOException {
            flush();
            closeConnection();
        }
    }

    @Override
    public void close() throws IOException {
        closeConnection();
    }

    private void closeConnection() throws IOException {
        key.cancel();
        channel.close();
    }

    public StringWriter getStringWriter() {
        if (this.tcpConnStringWriter == null) {
            this.tcpConnStringWriter = new TCPConnStringWriter();
        }
        return this.tcpConnStringWriter;
    }

    @Override
    void write() throws IOException {
        synchronized (pendingWrites) {
            while (!pendingWrites.isEmpty()) {
                ByteBuffer byteBuffer = pendingWrites.get(0);
                channel.write(byteBuffer);
                pendingWrites.remove(0);
            }
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }
}
