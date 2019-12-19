package de.tum.i13.server.kv.query;

public abstract class Query {
    private String key;
    private String value;

    abstract Result execute();

    public boolean isReadOnly() {
        return false;
    }

    Query setKey(String key) {
        this.key = key;
        return this;
    }

    Query setValue(String value) {
        this.value = value;
        return this;
    }
}
