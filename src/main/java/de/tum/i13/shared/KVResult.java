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

    /**
     * Decodes the item value, if present. Assumes that the value of this instance is base64 encoded.
     *
     * @return A new instance with a decoded value, if an item is present.
     *      If no item is available, this instance is returned.
     */
    public KVResult decoded() {
        if (this.item == null) {
            return this;
        }
        KVItem decodedItem = new KVItem(this.item.getKey());
        decodedItem.setValueFrom64(this.item.getValue());
        return new KVResult(message, decodedItem);
    }

    @Override
    public String toString() {
        String itemStr = (this.item != null) ? " " + this.item.toString() : "";
        return this.message + itemStr;
    }
}
