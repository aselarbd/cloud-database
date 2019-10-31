package de.tum.i13.shared;

/**
 * Wrapper class for KV server results. Contains a message (like put_success) and the assigned {@link KVItem}, if
 * any.
 */
public class KVResult {
    KVItem item;
    String message;

    public KVResult(String message, KVItem item) {
        this.message = message;
        this.item = item;
    }

    public KVResult(String message) {
        this(message, null);
    }

    public KVItem getItem() {
        return item;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        String itemStr = (this.item != null) ? " " + this.item.toString() : "";
        return this.message + itemStr;
    }
}
