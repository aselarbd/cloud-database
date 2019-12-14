package de.tum.i13.server.kv;

import de.tum.i13.server.kv.caches.FIFOCache;
import de.tum.i13.server.kv.caches.LFUCache;
import de.tum.i13.server.kv.caches.LRUCache;

/**
 * CacheBuilder configures and builds a new cache instance
 * with a given displacement strategy. The config methods return
 * a CacheBuilder instance, so the calls can be changed and the
 * cache will only be build and returned when the build method
 * is called.
 */
public final class CacheBuilder {

    public enum Algorithm {
        FIFO() {
            @Override
            public KVCache buildCache(int size) {
                return new FIFOCache(size);
            }
        },
        LRU() {
            @Override
            public KVCache buildCache(int size) {
                return new LRUCache(size);
            }
        },
        LFU() {
            @Override
            public KVCache buildCache(int size) {
                return new LFUCache(size);
            }
        };

        public abstract KVCache buildCache(int size);

        Algorithm() {
        }
    }

    private int size;
    private Algorithm algorithm;

    public static CacheBuilder newBuilder() {
        return new CacheBuilder();
    }

    public CacheBuilder size(int size) {
        this.size = size;
        return this;
    }

    public CacheBuilder algorithm(CacheBuilder.Algorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public KVCache build() {
        return this.algorithm.buildCache(this.size);
    }

}
