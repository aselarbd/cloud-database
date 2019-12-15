package de.tum.i13.client;


import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.client.communication.impl.SocketStreamCloser;
import de.tum.i13.shared.*;
import de.tum.i13.shared.parsers.KVResultParser;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Library to interact with a key-value server.
 */
public class KVLib {
    private final KVResultParser parser;
    private final static Logger LOGGER = Logger.getLogger(KVLib.class.getName());

    private ConsistentHashMap keyRanges;
    private ConsistentHashMap keyRangesReplica;
    private final Supplier<SocketCommunicator> communicatorFactory;
    private Map<InetSocketAddress, SocketCommunicator> communicatorMap = new HashMap<>();

    private final Map<String, Integer> requestFailureCounts = new HashMap<>();
    private static final int MAX_RETRIES = 10;

    public KVLib() {
        // Use a factory which returns new SocketCommunicatorImpl instances by default
        this(SocketCommunicatorImpl::new);
    }

    public KVLib(Supplier<SocketCommunicator> communicatorFactory) {
        this.parser = new KVResultParser();
        this.communicatorFactory = communicatorFactory;
        this.requestFailureCounts.put("put", 0);
        this.requestFailureCounts.put("get", 0);
        this.requestFailureCounts.put("delete", 0);
    }

    /**
     * get key range for new servers
     *
     * @return key range for each server that got by server or server stop message
     * if server not responding
     */
    public String keyRange() {
        getKeyRanges();
        if (keyRanges == null){
            LOGGER.log(Level.WARNING, "Metadata table on the kV Client is empty");
            return "Server Doesn't have key range values";
        }
        LOGGER.log(Level.INFO, "Generate Key range");
        return keyRanges.getKeyrangeString();
    }

    /**
     * Connect to the given server.
     *
     * @param address The server IP address as String
     * @param port The port number as int
     *
     * @return the message returned by the server or
     * a useful error message if no connection could be established
     *
     * @throws SocketCommunicatorException if the connection fails.
     */
    public String connect(String address, int port) throws SocketCommunicatorException {
        InetSocketAddress addr = new InetSocketAddress(address, port);
        if (communicatorMap.get(addr) != null) {
            return "already connected";
        }
        SocketCommunicator communicator = this.communicatorFactory.get();
        communicator.init(SocketStreamCloser::new, Constants.TELNET_ENCODING);
        String res = communicator.connect(address, port);
        communicatorMap.put(new InetSocketAddress(address, port), communicator);
        getKeyRanges();
        return res;
    }

    private void dropCommunicator(InetSocketAddress address) {
        SocketCommunicator comm = communicatorMap.get(address);
        if (comm.isConnected()) {
            try {
                comm.disconnect();
            } catch (SocketCommunicatorException e) {
                // no problem, we want to drop the communicator anyway
            }
        }
        communicatorMap.remove(address);
    }

    private void getKeyRanges() {
        if (!communicatorMap.isEmpty()) {
            Iterator<Map.Entry<InetSocketAddress,SocketCommunicator>> it = communicatorMap.entrySet().iterator();
            while (it.hasNext()){
                    Map.Entry<InetSocketAddress,SocketCommunicator> anyCom = it.next();
                try {
                    String keyRangeResponse = anyCom.getValue().send("keyrange");
                    if (keyRangeResponse.equals("server_stopped")) {
                        continue;
                    }
                    String keyRangeString = keyRangeResponse.split("\\s+")[1];
                    keyRanges = ConsistentHashMap.fromKeyrangeString(keyRangeString);
                    keyRangeResponse = anyCom.getValue().send("keyrange_read");
                    keyRangeString = keyRangeResponse.split("\\s+")[1];
                    // should usually not happen, but it is possible the server just got stopped. Ask another one
                    // even if we already got the write-keyrange.
                    if (keyRangeString.equals("server_stopped")) {
                        continue;
                    }
                    keyRangesReplica = ConsistentHashMap.fromKeyrangeReadString(keyRangeString);
                    return;
                } catch (SocketCommunicatorException e) {
                    it.remove();
                }
            }
        }
        // everything is empty. Reset keyranges and communicator map
        keyRanges = null;
        communicatorMap = new HashMap<>();
    }

    private void incrementFailures(String op) {
        requestFailureCounts.put(op, requestFailureCounts.get(op) + 1);
    }

    /**
     * Common logic for all operations
     *
     * @param op operation name (get, put, delete)
     * @param item Item to be processed
     * @return Server reply encoded as {@link KVResult}
     */
    private KVResult kvOperation(String op, KVItem item) {
        if (item == null || !item.isValid() || (op.equals("put") && item.getValue() == null)) {
            return new KVResult("Invalid key-value item");
        }
        if (keyRanges == null || keyRanges.size() <= 0) {
            return new KVResult("no server started");
        }
        if (requestFailureCounts.get(op) > MAX_RETRIES) {
            requestFailureCounts.put(op, 0);
            LOGGER.info("Exceeded maximum retries. Aborting.");
            return new KVResult("Server error");
        }

        final InetSocketAddress targetServer;

        if (op.equals("get")){
            List <InetSocketAddress> ipList = keyRangesReplica.getAllSuccessors(item.getKey());
            targetServer = ipList.get(new Random().nextInt(ipList.size()));
        } else {
            targetServer = keyRanges.getSuccessor(item.getKey());
        }

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                incrementFailures(op);
                getKeyRanges();
                return kvOperation(op, item);
            }
        }

        SocketCommunicator communicator = communicatorMap.get(targetServer);

        if (!communicator.isConnected()) {
            return new KVResult("not connected");
        }
        try {
            // in case of put, we need to encode the value first
            KVItem sendItem;
            if (op.equals("put")) {
                sendItem = new KVItem(item.getKey(), item.getValueAs64());
                // check encoded length again
                if (!sendItem.isValid()) {
                    return new KVResult("Value too long");
                }
            } else {
                sendItem = item;
            }
            String result = communicator.send(op + " " + sendItem.toString());
            KVResult res = parser.parse(result);
            if (res == null || op.equals("get") && res.getItem() == null) {
                requestFailureCounts.put(op, 0);
                return new KVResult("Empty response");
            } else if (res.getMessage().equals("get_error")) {
                requestFailureCounts.put(op, 0);
                return res;
            } else if (res.getMessage().equals("server_not_responsible") ||
                    res.getMessage().equals("server_stopped")) {
                incrementFailures(op);
                int timeout = getBackOffTime(requestFailureCounts.get(op));
                Thread.sleep(timeout);
                getKeyRanges();
                return kvOperation(op, item);
            } else if (!op.equals("get") && res.getMessage().equals("server_write_lock")) {
                requestFailureCounts.put(op, 0);
                return new KVResult("server locked, please try later");
            }
            requestFailureCounts.put(op, 0);
            // decode the value if present
            return res.decoded();
        } catch (SocketCommunicatorException e) {
            // server might just got removed, retry once before aborting
            if (requestFailureCounts.get(op) < MAX_RETRIES - 1) {
                requestFailureCounts.put(op, MAX_RETRIES - 1);
                // delete the communicator and update key ranges
                dropCommunicator(targetServer);
                getKeyRanges();
                return kvOperation(op, item);
            } else {
                LOGGER.log(Level.WARNING, "Error in put()", e);
                requestFailureCounts.put(op, 0);
                return new KVResult("Server error");
            }
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Error in put() -> timeout()", e);
            requestFailureCounts.put(op, 0);
            return new KVResult("Server error");
        }
    }


    /**
     * Put a key-value pair to the server.
     *
     * @param item the Key-Value item to put
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult put(KVItem item) {
        return kvOperation("put", item);
    }

    /**
     * Get a key-value pair
     * @param item Key to query, given as {@link KVItem}. The value is ignored.
     * @return Server reply encoded as {@link KVResult}, which also contains the respective {@link KVItem} if
     *  found.
     */
    public KVResult get(KVItem item) {
        return kvOperation("get", item);
    }

    /**
     * Delete a key-value pair.
     *
     * @param item Key to query, given as {@link KVItem}. The value is ignored.
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult delete(KVItem item) {
        return kvOperation("delete", item);
    }

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    public String disconnect() {
        StringBuilder res = new StringBuilder();
        for (Map.Entry<InetSocketAddress, SocketCommunicator> s : communicatorMap.entrySet()) {
            try {
                s.getValue().disconnect();
                res.append("Disconnected from ").append(InetSocketAddressTypeConverter.addrString(s.getKey())).append("\n");
            } catch (SocketCommunicatorException e) {
                LOGGER.log(Level.WARNING, "Could not close connection", e);
                res.append("Unable to disconnect from ").append(InetSocketAddressTypeConverter.addrString(s.getKey())).append(" - ").append(e.getMessage()).append("\n");
            }
        }
        // use an empty map again for the next connection
        communicatorMap = new HashMap<>();
        return res.toString();
    }

    /**
     * gives backoff time for client requests
     * @param attempt: no of current attempts
     * @return backoff time as int
     */
    private int getBackOffTime(int attempt) {
        final int maxBackoffTime = 1000;
        final int baseBackOffTime = 10;

        int backOffTime = Math.min(maxBackoffTime, baseBackOffTime * 2 ^ attempt);
        return new Random().nextInt(backOffTime +1);
    }

}
