package de.tum.i13.kvtp2;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

public class TCPConnection extends Connection {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private static final int BUFFER_SIZE = 8 * 1024;

    private final Selector selector;
    private final SocketChannel channel;

    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    private BiConsumer<Writer, byte[]> receiver;

    public TCPConnection(Selector selector, SocketChannel channel, BiConsumer<Writer, byte[]> receiver) throws ClosedChannelException {
        this.selector = selector;
        this.channel = channel;
        this.receiver = receiver;

        this.readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

        this.key = channel.register(this.selector, SelectionKey.OP_READ, this);
    }

    @Override
    void connect() {

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
            receiver.accept(new TCPConnWriter(), data);
        }
        // drop message
    }

    public class TCPConnWriter extends Writer {

        @Override
        public void write(char[] cbuf, int off, int len) {
            String s = new String(cbuf, off, len);
            byte[] bytes = s.getBytes(ENCODING);
            writeBuffer.put(bytes, off, len);
        }

        @Override
        public void flush() {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            key.selector().wakeup();
        }

        @Override
        public void close() {
            flush();
        }
    }

    @Override
    void write() throws IOException {
        channel.write(writeBuffer.flip());
        writeBuffer.clear();
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }
}
