package de.tum.i13.client;


import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.client.communication.impl.SocketStreamCloserFactory;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.parsers.KVResultParser;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Library to interact with a key-value server.
 */
public class KVLib {
    private KVResultParser parser;
    private final static Logger LOGGER = Logger.getLogger(KVLib.class.getName());

    private ConsistentHashMap keyRanges;
    private Map<InetSocketAddress, SocketCommunicator> communicatorMap = new HashMap<>();

    public KVLib() {
        this.parser = new KVResultParser();
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
        SocketCommunicator communicator = new SocketCommunicatorImpl();
        communicator.init(new SocketStreamCloserFactory(), Constants.TELNET_ENCODING);
        String res = communicator.connect(address, port);
        communicatorMap.put(new InetSocketAddress(address, port), communicator);
        getKeyRanges();
        return res;
    }

    private void getKeyRanges() throws SocketCommunicatorException {
        if (!communicatorMap.isEmpty()) {
            Map.Entry<InetSocketAddress, SocketCommunicator> anyComm = communicatorMap.entrySet().iterator().next();
            String keyRangeString = anyComm.getValue().send("keyrange");
            keyRanges = ConsistentHashMap.fromKeyrangeString(keyRangeString);
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

        InetSocketAddress targetServer = keyRanges.get(item.getKey());

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                try {
                    getKeyRanges();
                } catch (SocketCommunicatorException ex) {
                    return new KVResult("Failed to connect to KVServer");
                }
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

                getKeyRanges();
                return put(item);
            } else if (res.getMessage().equals("server_write_lock")) {
                return new KVResult("server locked, please try later");
            }
            return res;
        } catch (SocketCommunicatorException e) {
            LOGGER.log(Level.WARNING, "Error in put()", e);
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

        InetSocketAddress targetServer = keyRanges.get(item.getKey());

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                try {
                    getKeyRanges();
                } catch (SocketCommunicatorException ex) {
                    return new KVResult("Failed to connect to KVServer");
                }
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
                getKeyRanges();
                return get(item);
            } else if (res.getMessage().equals("get_success")) {
                // if successful, we need to decode the value before returning it
                KVItem decodedItem = new KVItem(res.getItem().getKey());
                decodedItem.setValueFrom64(res.getItem().getValue());
                if (!decodedItem.isValid()) {
                    decodedItem = null;
                }
                return new KVResult(res.getMessage(), decodedItem);
            }
            return res;
        } catch (SocketCommunicatorException e) {
            LOGGER.log(Level.WARNING, "Error in get()", e);
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

        InetSocketAddress targetServer = keyRanges.get(item.getKey());

        if (!communicatorMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (SocketCommunicatorException e) {
                try {
                    getKeyRanges();
                } catch (SocketCommunicatorException ex) {
                    return new KVResult("Failed to connect to KVServer");
                }
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
            } else if (res.getMessage().equals("server_not_responsible") ||
                    res.getMessage().equals("server_stopped")) {
                getKeyRanges();
                return delete(item);
            } else if (res.getMessage().equals("server_write_lock")) {
                return new KVResult("server locked, please try later");
            }
            return res;
        } catch (SocketCommunicatorException e) {
            LOGGER.log(Level.WARNING, "Error in delete()", e);
            return new KVResult("Server error");
        }
    }

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    void disconnect() throws SocketCommunicatorException {
        for (Map.Entry<InetSocketAddress, SocketCommunicator> s : communicatorMap.entrySet()) {
            s.getValue().disconnect();
        }
    }

}
