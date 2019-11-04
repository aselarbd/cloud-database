package de.tum.i13.server.kv.stores;

import de.tum.i13.server.database.DatabaseManager;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.KVItem;

import java.io.IOException;

/**
 * DatabaseStore implements a KVStore using the database
 * package as a storage backend. It is basically an adapter
 * to that package.
 */
public class DatabaseStore implements KVStore {

    DatabaseManager db;

    public DatabaseStore(String directory) throws IOException {
        db = new DatabaseManager(directory);
    }

    @Override
    public String put(KVItem item) throws IOException {
        int result = db.put(item.getKey(), item.getValue());
        return result <= 0 ? "success" : "update";
    }

    @Override
    public KVItem get(String key) throws IOException {
        return new KVItem(key, db.get(key));
    }
}
