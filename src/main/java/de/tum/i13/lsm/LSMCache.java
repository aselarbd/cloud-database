package de.tum.i13.lsm;

import de.tum.i13.shared.KVItem;

import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSMCache is the in memory store of our LSMTree implementation.
 * It holds a sorted map of key value items and provides put and get access
 * to the items in the cache.
 */
public class LSMCache {
    TreeMap<String, KVItem> lsmCache = new TreeMap<>();

    ReadWriteLock rwl = new ReentrantReadWriteLock();

    /**
     * put a new item to the cache.
     *
     * @param item item is a KVItem which should be saved
     */
    public void put(KVItem item) {
        try {
            rwl.writeLock().lock();

            lsmCache.put(item.getKey(), item);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * get an item from the LSMCache
     *
     * @param key the key of the item to get
     *
     * @return the requested item or null if no such item is present in
     * the cache.
     */
    public KVItem get(String key) {
        try{
            rwl.readLock().lock();

            KVItem kvItem = lsmCache.get(key);
            if (kvItem != null) {
                return kvItem;
            }
            return null;
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * getSnapshot creates a current snapshot of the LSMCache and
     * replaces the internal cache by a new TreeMap
     *
     * @return the current state of the cache
     */
    public TreeMap<String, KVItem> getSnapshot() {
        rwl.writeLock().lock();

        try {
            TreeMap<String, KVItem> snapshot = lsmCache;
            lsmCache = new TreeMap<>();
            return snapshot;
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public TreeMap<String, KVItem> getShallowLsmCopy() {
        return new TreeMap<>(lsmCache);
    }

    /**
     * get the amount of items currently stored in the cache.
     *
     * @return the amount of items in the cache.
     */
    public int size() {
        return lsmCache.size();
    }
}
