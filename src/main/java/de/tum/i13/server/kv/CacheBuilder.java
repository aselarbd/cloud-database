package de.tum.i13.server.kv;

public final class CacheBuilder {

    public enum Algorithm {
        FIFO() {
            @Override
            public KVCache buildCache(int size) {
                return new FIFOCache(size);
            }
        };

        public abstract  KVCache buildCache(int size);
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
