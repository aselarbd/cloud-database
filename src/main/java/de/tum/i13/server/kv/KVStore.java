package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * KVStore provides a common interface for different persistent storage
 * implementations
 */
public interface KVStore {

    /**
     * put stores a new or updated KVItem in the persistent storage.
     *
     * @param item item to store or update
     *
     * @return "success" if a new value was saved, "update" if a value was updated
     *
     * @throws IOException if some error occurs on IO
     */
    String put(KVItem item) throws IOException;

    /**
     * get a value from a persistent storage.
     *
     * @param key key of the requested item
     *
     * @return the requested item or null if no such item is present
     *
     * @throws IOException if some error occurs on IO
     */
    KVItem get(String key) throws IOException;

    Set<String> getAllKeys(Predicate<String> predicate) throws IOException;
}
