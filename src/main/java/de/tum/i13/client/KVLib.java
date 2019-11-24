package de.tum.i13.client;


import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.client.communication.impl.SocketStreamCloser;
import de.tum.i13.shared.*;
import de.tum.i13.shared.parsers.KVResultParser;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Library to interact with a key-value server.
 */
public class KVLib {
    private KVResultParser parser;
    private final static Logger LOGGER = Logger.getLogger(KVLib.class.getName());

    private ConsistentHashMap keyRanges;
    private Factory<SocketCommunicator> communicatorFactory;
    private Map<InetSocketAddress, SocketCommunicator> communicatorMap = new HashMap<>();

    private int getRequestFailureCount = 0;
    private int putRequestFailureCount = 0;
    private int deleteRequestFailureCount = 0;

    public KVLib() {
        // Use a factory which returns new SocketCommunicatorImpl instances by default
        this(SocketCommunicatorImpl::new);
    }

    public KVLib(Factory<SocketCommunicator> communicatorFactory) {
        this.parser = new KVResultParser();
        this.communicatorFactory = communicatorFactory;
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
    String connect(String address, int port) throws SocketCommunicatorException {
        InetSocketAddress addr = new InetSocketAddress(address, port);
        if (communicatorMap.get(addr) != null) {
            return "already connected";
        }
        SocketCommunicator communicator = this.communicatorFactory.getInstance();
        communicator.init(SocketStreamCloser::new, Constants.TELNET_ENCODING);
        String res = communicator.connect(address, port);
        communicatorMap.put(new InetSocketAddress(address, port), communicator);
        getKeyRanges();
        return res;
    }

    private void getKeyRanges() {
        if (!communicatorMap.isEmpty()) {
            Iterator<Map.Entry<InetSocketAddress,SocketCommunicator>> it = communicatorMap.entrySet().iterator();
            while (it.hasNext()){
                    Map.Entry<InetSocketAddress,SocketCommunicator> anyCom = it.next();
                try {
                    String keyRangeString = anyCom.getValue().send("keyrange");
                    if (keyRangeString.equals("server_stopped")) {
                        continue;
                    }
                    keyRanges = ConsistentHashMap.fromKeyrangeString(keyRangeString);
                    return;
                } catch (SocketCommunicatorException e) {
                    it.remove();
                }
            }
            communicatorMap = new HashMap<>();
        }
    }


    /**
     * Put a key-value pair to the server.
     *
     * @param item the Key-Value item to put
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult put(KVItem item) {

        if (item == null || !item.isValid() || item.getValue() == null) {
            return new KVResult("Invalid key-value item");
        }
        if (keyRanges == null || keyRanges.size() <= 0) {
            return new KVResult("no server started");
        }

        InetSocketAddress targetServer = keyRanges.getSuccessor(item.getKey());

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                putRequestFailureCount ++;
                getKeyRanges();
                return put(item);
            }
        }

        SocketCommunicator communicator = communicatorMap.get(targetServer);

        if (!communicator.isConnected()) {
            return new KVResult("not connected");
        }
        try {
            KVItem sendItem = new KVItem(item.getKey(), item.getValueAs64());
            // check encoded length again
            if (!sendItem.isValid()) {
                return new KVResult("Value too long");
            }
            String result = communicator.send("put " + sendItem.toString());
            KVResult res = parser.parse(result);
            if (res == null) {
                return new KVResult("Empty response");
            } else if (res.getMessage().equals("server_not_responsible") ||
                        res.getMessage().equals("server_stopped")) {

                putRequestFailureCount++;
                int timeout = getBackOffTime(putRequestFailureCount);
                communicator.setTimeOut(timeout);
                getKeyRanges();
                return put(item);
            } else if (res.getMessage().equals("server_write_lock")) {
                putRequestFailureCount =0;
                return new KVResult("server locked, please try later");
            }
            putRequestFailureCount =0;
            // decode the value if present
            return res.decoded();
        } catch (SocketCommunicatorException e) {
            LOGGER.log(Level.WARNING, "Error in put()", e);
            putRequestFailureCount =0;
            return new KVResult("Server error");
        } catch (SocketException e) {
            LOGGER.log(Level.WARNING, "Error in put() -> timeout()", e);
            putRequestFailureCount =0;
            return new KVResult("Server error");
        }
    }

    /**
     * Get a key-value pair
     * @param item Key to query, given as {@link KVItem}. The value is ignored.
     * @return Server reply encoded as {@link KVResult}, which also contains the respective {@link KVItem} if
     *  found.
     */
    public KVResult get(KVItem item) {

        if (!item.isValid()) {
            return new KVResult("Invalid key");
        }
        if (keyRanges == null || keyRanges.size() <= 0) {
            return new KVResult("no server started");
        }

        InetSocketAddress targetServer = keyRanges.getSuccessor(item.getKey());

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                getRequestFailureCount++;
                getKeyRanges();
                return get(item);
            }
        }

        SocketCommunicator communicator = communicatorMap.get(targetServer);

        if (!communicator.isConnected()) {
            return new KVResult("not connected");
        }
        try {
            String result = communicator.send("get " + item.getKey());
            KVResult res = parser.parse(result);
            if (res == null || res.getItem() == null) {
                res = new KVResult("Empty response");
            } else if (res.getMessage().equals("server_not_responsible") ||
                        res.getMessage().equals("server_stopped")) {
                getRequestFailureCount++;
                int timeout = getBackOffTime(getRequestFailureCount);
                communicator.setTimeOut(timeout);
                getKeyRanges();
                return get(item);
            }
            getRequestFailureCount =0;
            // decode the value if present
            return res.decoded();
        } catch (SocketCommunicatorException e) {
            getRequestFailureCount =0;
            LOGGER.log(Level.WARNING, "Error in get()", e);
            return new KVResult("Server error");
        } catch (SocketException e) {
            getRequestFailureCount =0;
            LOGGER.log(Level.WARNING, "error in get()-> timeout()");
            return new KVResult("Server error");
        }
    }

    /**
     * Delete a key-value pair.
     *
     * @param item Key to query, given as {@link KVItem}. The value is ignored.
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult delete(KVItem item) {

        if (!item.isValid()) {
            return new KVResult("Invalid key");
        }
        if (keyRanges == null || keyRanges.size() <= 0) {
            return new KVResult("no server started");
        }

        InetSocketAddress targetServer = keyRanges.getSuccessor(item.getKey());

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                deleteRequestFailureCount++;
                getKeyRanges();
                return delete(item);
            }
        }

        SocketCommunicator communicator = communicatorMap.get(targetServer);

        if (!communicator.isConnected()) {
            return new KVResult("not connected");
        }

        try {
            String result = communicator.send("delete " + item.getKey());
            KVResult res = parser.parse(result);
            if (res == null) {
                res = new KVResult("Empty response");
            } else if (res.getMessage().equals("server_not_responsible") || res.getMessage().equals("server_stopped")) {
                deleteRequestFailureCount++;
                int timeout = getBackOffTime(deleteRequestFailureCount);
                communicator.setTimeOut(timeout);
                getKeyRanges();
                return delete(item);
            } else if (res.getMessage().equals("server_write_lock")) {
                deleteRequestFailureCount =0;
                return new KVResult("server locked, please try later");
            }
            deleteRequestFailureCount =0;
            return res;
        } catch (SocketCommunicatorException e) {
            deleteRequestFailureCount =0;
            LOGGER.log(Level.WARNING, "Error in delete()", e);
            return new KVResult("Server error");
        } catch (SocketException e) {
            deleteRequestFailureCount =0;
            LOGGER.log(Level.WARNING, "Error in delete() -> timeout()", e);
            return new KVResult("Server error");
        }
    }

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    public String disconnect() {
        String res = "";
        for (Map.Entry<InetSocketAddress, SocketCommunicator> s : communicatorMap.entrySet()) {
            try {
                s.getValue().disconnect();
                res += "Disconnected from " + InetSocketAddressTypeConverter.addrString(s.getKey()) + "\n";
            } catch (SocketCommunicatorException e) {
                LOGGER.log(Level.WARNING, "Could not close connection", e);
                res += "Unable to disconnect from " + InetSocketAddressTypeConverter.addrString(s.getKey())
                        + " - " + e.getMessage() + "\n";
            }
        }
        // use an empty map again for the next connection
        communicatorMap = new HashMap<>();
        return res;
    }

    private int getBackOffTime(int attempt) {
        final int maxBackoffTime = 1000;
        final int baseBackOffTime = 10;

        int backOffTime = Math.min(maxBackoffTime, baseBackOffTime * 2 ^ attempt);
        Random random = new Random();
        return random.ints(0,(backOffTime+1)).findFirst().getAsInt();
    }

}
