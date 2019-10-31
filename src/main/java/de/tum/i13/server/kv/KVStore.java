package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

public interface KVStore {

    String put(KVItem item);

    String get(String key);

    String delete(KVItem item);
}
