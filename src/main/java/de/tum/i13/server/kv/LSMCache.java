package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMCache {
    TreeMap<String, KVItem> lsmCache = new TreeMap<>();

    ReadWriteLock rwl = new ReentrantReadWriteLock();

    public String put(KVItem item) {

        String result = lsmCache.containsKey(item.getKey()) ? "update" : "success";

        try {
            rwl.writeLock().lock();

            lsmCache.put(item.getKey(), item);
        } finally {
            rwl.writeLock().unlock();
        }

        return result;
    }

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

    public int size() {
        return lsmCache.size();
    }
}
