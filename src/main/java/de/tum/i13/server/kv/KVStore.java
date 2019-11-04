package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.io.IOException;

public interface KVStore {

    String put(KVItem item) throws IOException;

    KVItem get(String key) throws IOException;
}
