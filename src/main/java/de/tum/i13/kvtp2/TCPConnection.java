package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

class TCPConnection extends Connection {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private static final int BUFFER_SIZE = 8 * 1024;

    private ByteBuffer readBuffer;

    private BiConsumer<StringWriter, byte[]> receiver;

    private SocketChannel channel;

    private TCPConnStringWriter tcpConnStringWriter;

    TCPConnection(SocketChannel channel, BiConsumer<StringWriter, byte[]> receiver) {
        super(channel);
        this.receiver = receiver;
        this.channel = (SocketChannel) super.channel;

        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);

        int ops = channel.isConnected() ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT;
        this.pendingChanges.add(new ChangeRequest(this, ops));
    }

    @Override
    void connect() throws IOException {
        channel.finishConnect();

        // only called from main client thread so this is fine
        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
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
        readBuffer.flip();
        readBuffer.get(data);
        readBuffer.clear();

        if (receiver != null) {
            receiver.accept(getStringWriter(), data);
        }
        // drop message
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
            key.cancel();
            channel.close();
        }
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
