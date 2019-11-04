package de.tum.i13.server.kv;

public final class CacheBuilder {

    public enum Algorithm {
        FIFO("FIFO") {
            @Override
            public KVCache buildCache(int size) {
                return new FIFOCache(size);
            }
        },
        LRU("LRU") {
            @Override
            public KVCache buildCache(int size) {
                return new LRUCache(size);
            }
        },
        LFU("LFU") {
            @Override
            public KVCache buildCache(int size) {
                return new LFUCache(size);
            }
        };

        private final String algorithm;

        public abstract KVCache buildCache(int size);

        Algorithm(String algorithm) {
            this.algorithm = algorithm;
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
