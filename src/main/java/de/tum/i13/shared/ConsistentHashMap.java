package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Class {@link ConsistentHashMap} offers a wrapper datastructure to
 * map MD5 Hashes to InetSocketAddresses. It's basically a wrapper around a TreeMap
 * offering necessary functionality for fast storage and lookups of Server addresses
 * and their MD5 hashes. Especially, it is possible to find the closest server for arbitrary
 * string hashes.
 */
public class ConsistentHashMap {

    private MessageDigest messageDigest;
    private TreeMap<String, InetSocketAddress> consistentHashMap = new TreeMap<>();

    /**
     * Create a new {@link ConsistentHashMap}.
     *
     * @throws NoSuchAlgorithmException when MD5 is not available
     * (should never happen)
     */
    public ConsistentHashMap() throws NoSuchAlgorithmException {
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

    private String addressHash(InetSocketAddress addr) {
        String toHash = InetSocketAddressTypeConverter.addrString(addr);
        return getMD5DigestHEX(toHash);
    }

    /**
     * Save a server entry in the HashMap. The key will be the HEX-String of the hash
     * of the given IP:Port string.
     *
     * @param addr The IP:Port value as InetSocketAddress
     */
    public void put(InetSocketAddress addr) {
        consistentHashMap.put(addressHash(addr), addr);
    }

    /**
     * Get a key from the map given a plaintext key. The key will be hashed.
     * Afterwards this function returns value, which is stored under the
     * lexicographically next (hash-)key. If no such key exists, it returns the
     * first available key.
     *
     * @param key plain text key
     * @return The server address saved under the key, which lexicographically next to
     * the hashed key or the first, if no such key exists, or null if the map
     * is empty.
     */
    public InetSocketAddress get(String key) {
        Map.Entry<String, InetSocketAddress> ceiling = consistentHashMap.ceilingEntry(getMD5DigestHEX(key));
        if (ceiling != null) {
            return ceiling.getValue();
        }
        return consistentHashMap.firstEntry().getValue();
    }

    /**
     * remove a key value pair from the map.
     * @param addr The server address to remove
     */
    public void remove(InetSocketAddress addr) {
        consistentHashMap.remove(addressHash(addr));
    }

    /**
     * Get keyrange arguments in the format start_hash,end_hash,IP:Port;start_hash,...
     * @return
     */
    public String getKeyrangeString() {
        String items = "";
        String startHash = "";
        String endHash = "";
        String ipPort = "";
        for (Map.Entry<String, InetSocketAddress> entry : consistentHashMap.entrySet()) {
            // current hash is end hash for previous one
            endHash = entry.getKey();
            if (!startHash.equals("")) {
                items += startHash + "," + endHash + "," + ipPort + ";";
            }
            // prepare items to be written in the next iteration
            startHash = entry.getKey();
            ipPort = InetSocketAddressTypeConverter.addrString(entry.getValue());
        }
        // process the last item - it has the first item's hash as end hash
        endHash = consistentHashMap.firstKey();
        if (!startHash.equals("")) {
            items += startHash + "," + endHash + "," + ipPort + ";";
        }
        return items;
    }
}
