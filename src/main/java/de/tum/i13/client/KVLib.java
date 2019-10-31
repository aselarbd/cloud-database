package de.tum.i13.client;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.client.communication.impl.SocketStreamCloserFactory;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.parsers.KVResultParser;

/**
 * Library to interact with a key-value server.
 */
public class KVLib {
    private SocketCommunicator communicator;
    private KVResultParser parser;

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
        return this.communicator.connect(address, port);
    }

    /**
     * Put a key-value pair to the server.
     *
     * @param item the Key-Value item to put
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult put(KVItem item) {
        //TODO
        return null;
    }

    /**
     * Get a key-value pair
     * @param key Key to query.
     * @return Server reply encoded as {@link KVResult}, which also contains the respective {@link KVItem} if
     *  found.
     */
    public KVResult get(String key) {
        // TODO
        return null;
    }

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    void disconnect() throws SocketCommunicatorException {
        this.communicator.disconnect();
    }

}
