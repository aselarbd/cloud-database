package de.tum.i13.shared;

import java.util.Base64;

/**
 * Holds a key-value pair. The value is optional and defaults to null.
 */
public class KVItem {
    private String key;
    private String value = null;
    private static final int KEY_MAX_BYTES = 20;
    private static final int VAL_MAX_BYTES = 120000;

    public KVItem(String key) {
        this.key = key;
    }

    public KVItem(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public boolean isValid() {
        if (this.key.getBytes().length > KEY_MAX_BYTES) {
            return false;
        }
        if (this.value != null && this.value.getBytes().length > VAL_MAX_BYTES) {
            return false;
        }
        return true;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean hasValue() {
        return (value != null);
    }

    public String getValueAs64() {
        if (hasValue()) {
            return Base64.getEncoder().encodeToString(value.getBytes());
        }
        return null;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setValueFrom64(String rawInput) {
        this.value = new String(Base64.getDecoder().decode(rawInput.getBytes()));
    }
}
