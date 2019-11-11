package de.tum.i13.server.kv;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Class {@link ConsistentHashMap} offers a wrapper datastructure to
 * map MD5 Hashes to strings. It's basically a wrapper around a TreeMap
 * offering necessary functionality for fast storage and lookups of strings
 * and their MD5 hashes.
 */
public class ConsistentHashMap {

    private MessageDigest messageDigest;
    private TreeMap<String, String> consistentHashMap = new TreeMap<>();

    /**
     * Create a new {@link ConsistentHashMap}.
     *
     * @throws NoSuchAlgorithmException when MD5 is not available
     * (should never happen)
     */
    ConsistentHashMap() throws NoSuchAlgorithmException {
        this.messageDigest = MessageDigest.getInstance("MD5");
    }

    /**
     * getMD5DigestHEX returns the HEX-String representation of MD5 Hash
     * of a given string s.
     *
     * @param s The string to hash
     *
     * @return HEX-Representation of the MD5 hash of s
     */
    private String getMD5DigestHEX(String s) {
        messageDigest.reset();
        messageDigest.update(s.getBytes());
        byte[] digest = messageDigest.digest();

        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
        }
        return sb.toString();
    }

    /**
     * Save a string s in the HashMap. The key will be the HEX-String of the hash
     * of the given string.
     *
     * @param s String to save under it's own hash.
     */
    public void put(String s) {
        consistentHashMap.put(getMD5DigestHEX(s), s);
    }

    /**
     * Get a key from the map given a plaintext key. The key will be hashed.
     * Afterwards this function returns value, which is stored under the
     * lexicographically next (hash-)key. If no such key exists, it returns the
     * first available key.
     *
     * @param key plain text key
     * @return The value saved under the key, which lexicographically next to
     * the hashed key or the first, if no such key exists, or null if the map
     * is empty.
     */
    public String get(String key) {
        Map.Entry<String, String> ceiling = consistentHashMap.ceilingEntry(getMD5DigestHEX(key));
        if (ceiling != null) {
            return ceiling.getValue();
        }
        return consistentHashMap.firstEntry().getValue();
    }

    /**
     * remove a key value pair from the map.
     * @param ip
     */
    public void remove(String ip) {
        consistentHashMap.remove(getMD5DigestHEX(ip));
    }
}
