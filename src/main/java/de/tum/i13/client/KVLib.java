package de.tum.i13.client;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.client.communication.impl.SocketStreamCloserFactory;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.parsers.KVResultParser;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Library to interact with a key-value server.
 */
public class KVLib {
    private SocketCommunicator communicator;
    private KVResultParser parser;
    private final static Logger LOGGER = Logger.getLogger(KVLib.class.getName());

    public KVLib() {
        this.communicator = new SocketCommunicatorImpl();
        this.communicator.init(new SocketStreamCloserFactory(), Constants.TELNET_ENCODING);
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
        return communicator.connect(address, port);
    }

    /**
     * Put a key-value pair to the server.
     *
     * @param item the Key-Value item to put
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult put(KVItem item) {
        if (!communicator.isConnected()) {
            return new KVResult("not connected");
        }
        try {
            String result = communicator.send("put " + item.getKey() + " " + item.getValueAs64());
            return parser.parse(result);
        } catch (SocketCommunicatorException e) {
            LOGGER.log(Level.WARNING, "Error in put()", e);
            return new KVResult("Server error");
        }
    }

    /**
     * Get a key-value pair
     * @param key Key to query.
     * @return Server reply encoded as {@link KVResult}, which also contains the respective {@link KVItem} if
     *  found.
     */
    public KVResult get(String key) {
        if (!communicator.isConnected()) {
            return new KVResult("not connected");
        }
        try {
            String result = communicator.send("get " + key);
            KVResult res = parser.parse(result);
            if (res.getMessage().equals("get_success")) {
                // if successful, we need to decode the value before returning it
                KVItem decodedItem = new KVItem(res.getItem().getKey());
                decodedItem.setValueFrom64(res.getItem().getValue());
                return new KVResult(res.getMessage(), decodedItem);
            }
            return res;
        } catch (SocketCommunicatorException e) {
            LOGGER.log(Level.WARNING, "Error in get()", e);
            return new KVResult("Server error");
        }
    }

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    void disconnect() throws SocketCommunicatorException {
        communicator.disconnect();
    }

}
