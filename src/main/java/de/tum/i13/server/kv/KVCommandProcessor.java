package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.parsers.KVResultParser;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {

    private static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVCache kvCache;
    private KVStore kvStore;

    // TODO: initialize with values given by ecs
    private InetSocketAddress address;

    private ConsistentHashMap keyRange;

    private boolean writeLock;

    KVCommandProcessor(InetSocketAddress address, KVCache kvCache, KVStore kvStore) throws NoSuchAlgorithmException {
        this.address = address;
        keyRange = new ConsistentHashMap();

        this.kvCache = kvCache;
        this.kvStore = kvStore;
        this.writeLock = false;
    }

    @Override
    public String process(InetSocketAddress src, String input) {

        if (input.toLowerCase().equals("keyrange")) {
            return keyRange.getKeyrangeString();
        }

        KVResultParser parser = new KVResultParser();
        KVResult command = parser.parse(input);

        String key = command.getItem().getKey();
        InetSocketAddress targetServerAddress = keyRange.get(key);

        if (targetServerAddress == null) {
            return "server_stopped";
        }
        if (!targetServerAddress.equals(address)) {
            return "server_not_responsible";
        }
        if (!command.getMessage().toLowerCase().equals("get") && writeLock) {
            return "server_write_lock";
        }

        switch(command.getMessage().toLowerCase()) {
            case "get":
                return get(command.getItem().getKey());
            case "put":
                return put(command.getItem());
            case "delete":
                return delete(command.getItem());
            default:
                return "unknown command";
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
        return "KVCommandProcessor connected: " + address + " to " + remoteAddress;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }

    public void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRange = keyRange;
    }

    public ConsistentHashMap getKeyRange() {
        return this.keyRange;
    }

    public void setWriteLock() {
        this.writeLock = true;
    }

    public void releaseWriteLock() {
        this.writeLock = false;
    }

    public InetSocketAddress getAddr() {
        return address;
    }

}
