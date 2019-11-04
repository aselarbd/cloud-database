package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * FIFOCache provides A KVCache using FIFO as its displacement strategy.
 */
public class FIFOCache implements KVCache {

    private int size;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    private Map<String, KVItem> cache;
    private Queue<String> fifo;

    FIFOCache(int size) {
        this.size = size;
        this.cache = new HashMap<>();
        this.fifo = new LinkedList<>();
    }

    /**
     * Get a value from the cache.
     *
     * @param key of the requested value
     *
     * @return the requested value if present, null otherwise.
     */
    @Override
    public KVItem get(String key) {
        try {
            rwl.readLock().lock();

            return cache.get(key);
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * put a new value to the cache. Updates are also possible through this method.
     * An update does not put the value at the end of the FIFO queue, but leaves
     * the item at the same place as before.
     *
     * @param item the item to cache.
     */
    @Override
    public void put(KVItem item) {
        try {
            rwl.writeLock().lock();

            String key = item.getKey();
            if (cache.containsKey(key)) {
                cache.put(key, item);
                return;
            }

            if (fifo.size() >= size && size > 0) {
                String removed = fifo.remove();
                cache.remove(removed);
            }
            cache.put(key, item);
            fifo.add(key);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * Delete a value from the cache.
     *
     * @param item the item to delete
     */
    @Override
    public void delete(KVItem item) {
        try {
            rwl.writeLock().lock();

            String key = item.getKey();
            cache.remove(key);
            fifo.remove(key);
        } finally {
            rwl.writeLock().unlock();
        }
    }
}
