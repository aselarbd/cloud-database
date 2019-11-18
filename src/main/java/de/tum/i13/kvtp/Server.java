package de.tum.i13.kvtp;

import de.tum.i13.shared.Constants;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * Based on http://rox-xmlrpc.sourceforge.net/niotut/
 */
public class Server {

    private byte[] messageDelimiter;

    public static Logger logger = Logger.getLogger(Server.class.getName());

    private List<ChangeRequest> pendingChanges;
    private Map<SelectionKey, List<ByteBuffer>> pendingWrites;
    private Map<SelectionKey, byte[]> pendingReads;

    private Selector selector;

    private ByteBuffer readBuffer;

    private List<ServerSocketChannel> serverSocketChannels;
    private List<SocketChannel> socketChannels;

    public Server() throws UnsupportedEncodingException {
        this.serverSocketChannels = new ArrayList<>();
        this.socketChannels = new ArrayList<>();

        this.pendingChanges = new LinkedList<>();
        this.pendingWrites = new HashMap<>();
        this.pendingReads = new HashMap<>();

        this.readBuffer = ByteBuffer.allocate(8192); // = 2^13

        messageDelimiter = "\r\n".getBytes(Constants.TELNET_ENCODING);
    }

    public void bindSockets(String serverName, int port, CommandProcessor cmdProcessor) throws IOException {
        // Create a new non-blocking server selectionKey channel
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);

        // Bind the server selectionKey to the specified address and port
        InetSocketAddress isa = new InetSocketAddress(InetAddress.getByName(serverName), port);
        ssc.socket().bind(isa);

        // Register the server selectionKey channel, indicating an interest in
        // accepting new connections
        if (this.selector == null) {
            this.selector = SelectorProvider.provider().openSelector();
        }

        SelectionKey sk = ssc.register(selector, SelectionKey.OP_ACCEPT);
        sk.attach(cmdProcessor);
        serverSocketChannels.add(ssc);
    }

    public void start() throws IOException {
        while (true) {
            // Process queued interest changes
            for (ChangeRequest change : this.pendingChanges) {
                change.selectionKey.interestOps(change.ops);
            }
            this.pendingChanges.clear();

            // Wait for an event one of the registered channels
            this.selector.select();

            // Iterate over the set of keys for which events are available
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();

                if (!key.isValid()) {
                    continue;
                }

                // Check what event is available and deal with it
                if (key.isAcceptable()) {
                    accept(key);
                } else if (key.isReadable()) {
                    read(key);
                } else if (key.isWritable()) {
                    write(key);
                }
            }
        }
    }

    private void accept(SelectionKey key) throws IOException {

        // For an accept to be pending the channel must be a server selectionKey
        // channel.
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

        // Accept the connection and make it non-blocking
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);

        InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
        InetSocketAddress localAddress = (InetSocketAddress)socketChannel.getLocalAddress();
        CommandProcessor cmdProcessor = (CommandProcessor) key.attachment();
        String confirmation = cmdProcessor.connectionAccepted(localAddress, remoteAddress);

        // Register the new SocketChannel with our Selector, indicating
        // we'd like to be notified when there's data waiting to be read
        SelectionKey registeredKey = socketChannel.register(this.selector, SelectionKey.OP_WRITE);
        registeredKey.attach(cmdProcessor);
        queueForWrite(registeredKey, confirmation.getBytes(Constants.TELNET_ENCODING));
    }

    private void read(SelectionKey key) throws UnsupportedEncodingException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // Clear out our read buffer so it's ready for new data
        this.readBuffer.clear();

        // Attempt to read off the channel
        int numRead;
        try {
            numRead = socketChannel.read(this.readBuffer);
        } catch (IOException e) {
            logger.info("failed to read from remote, closing connection: " + e.getMessage());
            closeRemote(key);
            return;
        }

        if (numRead == -1) {
            logger.info("Read -1 bytes from buffer, socket has reached end-of-stream, closing connection");
            closeRemote(key);
            return;
        }

        byte[] dataCopy = new byte[numRead];
        System.arraycopy(this.readBuffer.array(), 0, dataCopy, 0, numRead);

        // If we have already received some data, we add this to our buffer
        if (this.pendingReads.containsKey(key)) {
            byte[] existingBytes = pendingReads.get(key);

            byte[] concatenated = new byte[existingBytes.length + dataCopy.length];
            System.arraycopy(existingBytes, 0, concatenated, 0, existingBytes.length);
            System.arraycopy(dataCopy, 0, concatenated, existingBytes.length, dataCopy.length);

            dataCopy = concatenated;
        }

        if(dataCopy.length > 128000) {
            logger.info("Remote message exceeded max message size, closing connection");
            closeRemote(key);
            this.pendingReads.remove(key);
            return;
        }

        // In case we have now finally reached all characters
        // Split by delimiter \r\n and handle all requests which are delimited
        // everything after is then put back to pending reads
        List<byte[]> buffers = splitByDelimiter(dataCopy, messageDelimiter);
        for (int i = 0; i < buffers.size() - 1; i++) {
            handleRequest(key, new String(buffers.get(i), Constants.TELNET_ENCODING));
        }
        this.pendingReads.put(key, buffers.get(buffers.size() - 1));
    }

    private static boolean match(byte[] pattern, byte[] input, int pos) {
        for (int i = 0; i < pattern.length; i++) {
            if (input[pos + i] != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    static List<byte[]> splitByDelimiter(byte[] data, byte[] delimiter) {
        List<byte[]> result = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < data.length; i++) {
            if (match(delimiter, data, i)) {
                byte[] block = new byte[i - start];
                System.arraycopy(data, start, block, 0, i - start);
                result.add(block);
                start = i + delimiter.length;
                i = start;
            }
        }

        byte[] last = new byte[data.length - start];
        System.arraycopy(data, start,last, 0, data.length - start);
        result.add(last);
        return result;
    }

    private void closeRemote(SelectionKey key) {
        SocketChannel sc = (SocketChannel) key.channel();
        CommandProcessor cp = (CommandProcessor) key.attachment();

        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) sc.getRemoteAddress();
            cp.connectionClosed(remoteAddress.getAddress());
            key.cancel();
            sc.close();
        } catch(ClosedChannelException cce) {
            logger.info("Failed to close remote, channel already closed " + cce.getMessage());
        } catch (IOException ioe) {
            logger.warning("Failed to close remote: " + ioe.getMessage());
        }
    }

    private void write(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        List<ByteBuffer> queue = this.pendingWrites.get(key);

        // Write until there's no more data left ...
        while (!queue.isEmpty()) {
            ByteBuffer buf = queue.get(0);
            try {
                socketChannel.write(buf);
            }catch (IOException ex) {
                //There could be an IOException: Connection reset by peer
                queue.clear(); //clear the queue
                this.pendingWrites.remove(key);
                closeRemote(key);
                return;
            }
            if (buf.remaining() > 0) {
                // ... or the selectionKey's buffer fills up
                break;
            }
            queue.remove(0);
        }

        if (queue.isEmpty()) {
            // We wrote away all data, so we're no longer interested
            // in writing on this selectionKey. Switch back to waiting for
            // data.
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    public void connectTo(String address, int port, CommandProcessor ecsProcessor) throws IOException {
        SocketChannel sc = SocketChannel.open();
        sc.connect(new InetSocketAddress(address, port));
        sc.configureBlocking(false);

        if (this.selector == null) {
            this.selector = SelectorProvider.provider().openSelector();
        }

        SelectionKey key = sc.register(selector, SelectionKey.OP_CONNECT);
        key.attach(ecsProcessor);
        this.socketChannels.add(sc);
    }

    private void handleRequest(SelectionKey selectionKey, String request) {
        CommandProcessor cp = (CommandProcessor) selectionKey.attachment();
        try {
            String res = cp.process(request) + "\r\n";
            send(selectionKey, res.getBytes(Constants.TELNET_ENCODING));
        } catch (UnsupportedEncodingException e) {
            logger.severe("Failed to send due to unsupported Encoding: " + e.getMessage());
        }
    }

    private void send(SelectionKey selectionKey, byte[] data) {
        // Indicate we want the interest ops set changed
        this.pendingChanges.add(new ChangeRequest(selectionKey, SelectionKey.OP_WRITE));

        // And queue the data we want written
        queueForWrite(selectionKey, data);

        // Finally, wake up our selecting thread so it can make the required
        // changes
        this.selector.wakeup();
    }

    private void queueForWrite(SelectionKey selectionKey, byte[] data) {
        List<ByteBuffer> queue = this.pendingWrites.computeIfAbsent(selectionKey, k -> new ArrayList<>());
        queue.add(ByteBuffer.wrap(data));
    }

    public void close() {
        for (ServerSocketChannel ssc : serverSocketChannels) {
            try {
                ssc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
