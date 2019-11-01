package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    @Override
    public KVItem get(String key) {
        try {
            rwl.readLock().lock();

            return cache.get(key);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void put(KVItem item) {
        try {
            rwl.writeLock().lock();

            String key = item.getKey();
            if (cache.containsKey(key)) {
                cache.put(key, item);
                return;
            }

            if (fifo.size() >= size) {
                String removed = fifo.remove();
                cache.remove(removed);
            }
            cache.put(key, item);
            fifo.add(key);
        } finally {
            rwl.writeLock().unlock();
        }
    }

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
