package de.tum.i13.server.kv;

import de.tum.i13.server.database.DatabaseManager;
import de.tum.i13.shared.KVItem;

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
    public void put(KVItem item) {
        db.put(item.getKey(), item.getValue());
        cache.put(item);
    }

    @Override
    public KVItem get(String key) {
        KVItem kvItem = cache.get(key);
        if (kvItem != null) {
            return kvItem;
        }

        String value = db.get(key);
        if (value != null) {
            kvItem = new KVItem(key, value);
            cache.put(kvItem);
            return kvItem;
        }

        return null;
    }

    @Override
    public void delete(KVItem item) {
        cache.delete(item);
        db.delete(item.getKey());
    }
}
