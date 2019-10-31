package de.tum.i13.server.kv;

import de.tum.i13.server.database.DatabaseManager;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class KVStoreManager implements KVStore {

    private static Logger logger = Logger.getLogger(KVStoreManager.class.getName());

    private KVCache cache;
    private DatabaseManager db;

    public KVStoreManager(KVCache cache, DatabaseManager db) {
        this.cache = cache;
        this.db = db;
    }

    @Override
    public String put(KVItem item) {
        int put;
        try {
            put = db.put(item.getKey(), item.getValue());
        } catch (IOException e) {
            logger.severe("Could not put value to Database: " + e.getMessage());
            return "put_error " + item.getKey() + " " + item.getValue();
        }
        cache.put(item);

        return put <= 0 ? "put_success " + item.getKey() : "put_update " + item.getKey();
    }

    @Override
    public String get(String key) {
        KVItem kvItem = cache.get(key);
        if (kvItem != null) {
            return "get_success " + kvItem.getKey() + " " + kvItem.getValue();
        }

        String value;
        try {
            value = db.get(key);
        } catch (IOException e) {
            logger.severe("Could not get value from database: " + e.getMessage());
            return "get_error " + key;
        }

        if (value != null) {
            kvItem = new KVItem(key, value);
            cache.put(kvItem);
            return "get_success " + kvItem.getKey() + " " + kvItem.getValue();
        }

        return "get_error " + key;
    }

    @Override
    public String delete(KVItem item) {
        cache.delete(item);
        try {
            db.delete(item.getKey());
        } catch (IOException e) {
            logger.severe("Could not delete value from database: " + e.getMessage());
            return "delete_error " + item.getKey();
        }
        return "delete_success " + item.getKey();
    }
}
