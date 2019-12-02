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
import java.util.Set;
import java.util.function.Predicate;
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

    KVCommandProcessor(InetSocketAddress address, KVCache kvCache, KVStore kvStore) {
        this.address = address;
        keyRange = new ConsistentHashMap();

        this.kvCache = kvCache;
        this.kvStore = kvStore;
        this.writeLock = false;
    }

    @Override
    public String process(InetSocketAddress src, String input) {

        KVResultParser parser = new KVResultParser();
        KVResult command = parser.parse(input);

        if (command == null) {
            return "unknown command";
        }

        String cmdMsg = command.getMessage();

        if (cmdMsg.equals("keyrange")) {
            if (keyRange != null && keyRange.size() > 0) {
                return keyRange.getKeyrangeString();
            } else {
                return "server_stopped";
            }
        }

        KVItem item = command.getItem();
        if (item == null) {
            // TODO: probably return more specific get_error etc.
            return "key needed";
        }
        String key = item.getKey();

        if (keyRange == null || keyRange.getSuccessor(key) == null) {
            return "server_stopped";
        }
        if (!keyRange.getSuccessor(key).equals(address)) {
            return "server_not_responsible";
        }
        if (!cmdMsg.toLowerCase().equals("get") && writeLock) {
            return "server_write_lock";
        }

        switch(cmdMsg.toLowerCase()) {
            case "get":
                return get(item.getKey());
            case "put":
                return put(item);
            case "delete":
                return delete(item);
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
            return "put_error " + item.toString();
        }
        kvCache.put(item);

        return "put_" + result + " " + item.getKey() ;
    }

    public KVItem getItem(String key) {
        KVItem kvItem = kvCache.get(key);
        if (kvItem != null) {
            return kvItem;
        }

        KVItem value;
        try {
            value = kvStore.get(key);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not get value from Database", e);
            return null;
        }

        if (value != null) {
            return new KVItem(key, value.getValue());
        }
        return null;
    }

    private String get(String key) {
        KVItem kvItem = kvCache.get(key);
        if (kvItem != null) {
            return "get_success " + kvItem.toString();
        }

        KVItem value;
        try {
            value = kvStore.get(key);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not get value from Database", e);
            KVItem kvItemError = new KVItem(key, "Could not get value from Database");
            return "get_error " + kvItemError.toString();
        }

        if (value != null) {
            kvItem = new KVItem(key, value.getValue());
            kvCache.put(kvItem);
            return "get_success " + kvItem.toString();
        }

        KVItem kvItemError = new KVItem(key, "Key not found in the Database");
        return "get_error " +  kvItemError.toString();
    }

    public String delete(KVItem item) {
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

    public Set<String> getAllKeys(Predicate<String> predicate) {
        try {
            return kvStore.getAllKeys(predicate);
        } catch (IOException e) {
            logger.warning("Could not read all keys for predicate from disk");
        }
        return null;
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
