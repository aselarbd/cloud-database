package de.tum.i13.server.kv;

public final class CacheBuilder <K, V> {

    public enum Algorithm {
        FIFO() {
            @Override
            public <K, V> KVCache buildCache(int size) {
                return new FIFOCache(size);
            }
        };

        public abstract <K, V> KVCache buildCache(int size);
    }

    private int size;
    private Algorithm algorithm;

    public static CacheBuilder<Object, Object> newBuilder() {
        return new CacheBuilder<>();
    }

    public CacheBuilder<K, V> size(int size) {
        this.size = size;
        return this;
    }

    public CacheBuilder<K, V> algorithm(CacheBuilder.Algorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    public <K1 extends K, V1 extends V> KVCache build() {
        return this.algorithm.buildCache(this.size);
    }

}
