package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.parsers.KVResultParser;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {

    private static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    private KVCache kvCache;
    private KVStore kvStore;

    public KVCommandProcessor(KVCache kvCache, KVStore kvStore) {
        this.kvCache = kvCache;
        this.kvStore = kvStore;
    }

    @Override
    public String process(String input) {
        KVResultParser parser = new KVResultParser();
        KVResult command = parser.parse(input);

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
            logger.severe("Could not put value to Database: " + e.getMessage());
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

        String value;
        try {
            value = kvStore.get(key);
        } catch (IOException e) {
            logger.severe("Could not get value from database: " + e.getMessage());
            return "get_error " + key;
        }

        if (value != null) {
            kvItem = new KVItem(key, value);
            kvCache.put(kvItem);
            return "get_success " + kvItem.getKey() + " " + kvItem.getValue();
        }

        return "get_error " + key;
    }

    private String delete(KVItem item) {
        kvCache.delete(item);
        try {
            kvStore.put(new KVItem(item.getKey(), Constants.DELETE_MARKER));
        } catch (IOException e) {
            logger.severe("Could not delete value from database: " + e.getMessage());
            return "delete_error " + item.getKey();
        }
        return "delete_success " + item.getKey();
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        //TODO

        return null;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        //TODO

    }
}
