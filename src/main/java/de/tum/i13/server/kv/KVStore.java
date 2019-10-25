package de.tum.i13.server.kv;

public interface KVStore {

    void put(String key, String value);

    String get(String key);

}
