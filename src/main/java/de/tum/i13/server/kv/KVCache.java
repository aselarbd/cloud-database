package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

/**
 * KVCache provides a common interface for different caching mechanisms
 */
public interface KVCache {

    /**
     * get a key from the cache
     *
     * @param key of the value to get
     *
     * @return the value or null if it is not present
     */
    KVItem get(String key);

    /**
     * put a new item to the cache
     *
     * @param item the item to store in the cache
     */
    void put(KVItem item);

    /**
     * delete a value from the cache
     *
     * @param key the key of the item to delete
     */
    void delete(String key);
}