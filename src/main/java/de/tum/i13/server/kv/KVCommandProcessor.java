package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.shared.*;
import de.tum.i13.shared.parsers.KVResultParser;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {

    private static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVCache kvCache;
    private KVStore kvStore;

    // TODO: initialize with values given by ecs
    private String address;
    private ConsistentHashMap keyRange;

    private ReentrantReadWriteLock serverWriteLock = new ReentrantReadWriteLock();
    private boolean serverStopped;

    public KVCommandProcessor(KVCache kvCache, KVStore kvStore) throws NoSuchAlgorithmException {
        // TODO: set useful values
        address = "0";
        keyRange = new ConsistentHashMap();
        keyRange.put(address); // TODO: delete this line
        serverStopped = false;

        this.kvCache = kvCache;
        this.kvStore = kvStore;
    }

    @Override
    public String process(String input) {
        KVResultParser parser = new KVResultParser();
        KVResult command = parser.parse(input);

        String key = command.getItem().getKey();
        if (!keyRange.get(key).equals(address)) {
            return "server_not_responsible";
        }
        if (serverWriteLock.isWriteLocked()) {
            return "server_write_lock";
        }
        if (serverStopped) {
            return "server_stopped";
        }

        switch(command.getMessage().toLowerCase()) {
            case "get":
                return get(command.getItem().getKey()) + "\r\n";
            case "put":
                return put(command.getItem()) + "\r\n";
            case "delete":
                return delete(command.getItem()) + "\r\n";
            default:
                return "unknown command" + "\r\n";
        }
    }

    private String put(KVItem item) {
        String result;
        try {
            result = kvStore.put(item);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not put value to Database", e);
            return "put_error " + item.getKey() + " " + item.getValue();
        }
        kvCache.put(item);

        return "put_" + result + " " + item.getKey() + " " + item.getValue();
    }

    private String get(String key) {
        KVItem kvItem = kvCache.get(key);
        if (kvItem != null) {
            return "get_success " + kvItem.getKey() + " " + kvItem.getValue();
        }

        KVItem value;
        try {
            value = kvStore.get(key);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not get value from Database", e);
            return "get_error " + key;
        }

        if (value != null) {
            kvItem = new KVItem(key, value.getValue());
            kvCache.put(kvItem);
            return "get_success " + kvItem.getKey() + " " + kvItem.getValue();
        }

        return "get_error " + key;
    }

    private String delete(KVItem item) {
        try {
            if (kvStore.get(item.getKey()) != null) {
                kvStore.put(new KVItem(item.getKey(), Constants.DELETE_MARKER));
                kvCache.delete(item);
                return "delete_success " + item.getKey();
            }
            return "delete_error " + item.getKey();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not delete value from database", e);
            return "delete_error " + item.getKey();
        }
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());
        return "connected to KVServer: " + address.toString();
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }
}
