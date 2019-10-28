package de.tum.i13.server.kv;

import java.util.logging.Logger;

public class CachedKVStore implements KVStore {

    private static Logger logger = Logger.getLogger(CachedKVStore.class.getName());

    @Override
    public void put(String key, String value) {
        logger.info("put " + key + ":" + value);
    }

    @Override
    public String get(String key) {
        logger.info("get " + key);
        return null;
    }
}
