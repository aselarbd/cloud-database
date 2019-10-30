package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

public interface KVStore {

    void put(KVItem item);

    KVItem get(String key);

    void delete(KVItem item);
}
