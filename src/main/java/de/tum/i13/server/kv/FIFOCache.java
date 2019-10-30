package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

public class FIFOCache implements KVCache {

    private int size;

    FIFOCache(int size) {
        this.size = size;
    }

    @Override
    public KVItem get(String key) {
        return null;
    }

    @Override
    public void put(KVItem item) {

    }

    @Override
    public void delete(KVItem item) {

    }
}
