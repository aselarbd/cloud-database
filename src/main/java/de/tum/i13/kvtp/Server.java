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
    private Map<InetSocketAddress, SocketChannel> socketChannels;

    public Server() throws UnsupportedEncodingException {
        this.serverSocketChannels = new ArrayList<>();
        this.socketChannels = new HashMap<>();

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

    public void connectTo(InetSocketAddress address, CommandProcessor ecsProcessor) throws IOException {
        SocketChannel sc = SocketChannel.open();
        boolean connected = sc.connect(address);
        sc.configureBlocking(false);

        if (this.selector == null) {
            this.selector = SelectorProvider.provider().openSelector();
        }

        SelectionKey key = sc.register(selector, SelectionKey.OP_CONNECT);
        key.attach(ecsProcessor);
        this.socketChannels.put(address, sc);

        if (connected) {
            connect(key);
        }
    }

    public void sendTo(InetSocketAddress address, String message) {
        SelectionKey selectionKey = socketChannels.get(address).keyFor(this.selector);
        send(selectionKey, (message).getBytes(Constants.TELNET_ENCODING_CHARSET));
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
                } else if (key.isConnectable()) {
                    connect(key);
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

        socketChannels.put(remoteAddress, socketChannel);

        // Register the new SocketChannel with our Selector, indicating
        // we'd like to be notified when there's data waiting to be read
        SelectionKey registeredKey = socketChannel.register(this.selector, SelectionKey.OP_WRITE);
        registeredKey.attach(cmdProcessor);
        send(registeredKey, confirmation.getBytes(Constants.TELNET_ENCODING));
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
        byte[] unprocessed = processReceiveBuffer(dataCopy, key);
        this.pendingReads.put(key, unprocessed);
    }

    // This is telnet specific, maybe you have to change it according to your
    private byte[] processReceiveBuffer(byte[] data, SelectionKey key) throws UnsupportedEncodingException {
        int length = data.length;
        int start = 0;
        for(int i = 1; i < length; i++) {
            if(data[i] == '\n') {
                if(i > 1 && data[i-1] == '\r') {

                    byte[] concatenated = new byte[(i-1) - start];
                    System.arraycopy(data, start, concatenated, 0, (i-1) - start);

                    String tempStr = new String(concatenated, Constants.TELNET_ENCODING);

                    if (tempStr.length() > 0) {
                        handleRequest(key, tempStr);
                    }

                    start = i +1;
                }
            }
        }

        byte[] unprocessed = new byte[data.length - start];
        System.arraycopy(data, start, unprocessed, 0, unprocessed.length);

        return unprocessed;
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

    private void connect(SelectionKey key) throws IOException {
        SocketChannel sc = (SocketChannel) key.channel();
        sc.finishConnect();
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

    private void handleRequest(SelectionKey selectionKey, String request) {
        CommandProcessor cp = (CommandProcessor) selectionKey.attachment();
        try {
            SocketChannel sc = (SocketChannel) selectionKey.channel();
            InetSocketAddress remoteAddress = (InetSocketAddress) sc.getRemoteAddress();
            String res = cp.process(remoteAddress, request);
            if (res != null) {
                send(selectionKey, res.getBytes(Constants.TELNET_ENCODING_CHARSET));
            }
        } catch (IOException e) {
            logger.severe("Failed to send due to unsupported Encoding: " + e.getMessage());
        }
    }

    private void send(SelectionKey selectionKey, byte[] data) {
        // Indicate we want the interest ops set changed
        this.pendingChanges.add(new ChangeRequest(selectionKey, SelectionKey.OP_WRITE));

        byte[] concatenated = new byte[data.length + messageDelimiter.length];
        System.arraycopy(data, 0, concatenated, 0, data.length);
        System.arraycopy(messageDelimiter, 0, concatenated, data.length, messageDelimiter.length);

        // And queue the data we want written
        queueForWrite(selectionKey, concatenated);

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
