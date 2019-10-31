package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

public interface KVCache {

    KVItem get(String key);

    void put(KVItem item);

    void delete(KVItem item);
}