package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    ReadWriteLock rwl = new ReentrantReadWriteLock();

    /**
     * Create a new {@link ConsistentHashMap}.
     *
     */
    public ConsistentHashMap() {
        try {
            this.messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
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
        rwl.writeLock().lock();
        consistentHashMap.put(addressHash(addr), addr);
        rwl.writeLock().unlock();
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
        rwl.readLock().lock();
        Map.Entry<String, InetSocketAddress> ceiling = consistentHashMap.ceilingEntry(getMD5DigestHEX(key));
        if (ceiling == null) {
            ceiling = consistentHashMap.firstEntry();
        }
        rwl.readLock().unlock();
        return (ceiling == null) ? null : ceiling.getValue();
    }

    public InetSocketAddress get(InetSocketAddress key) {
        if (key != null) {
            return get(addressHash(key));
        }
        return null;
    }

    public InetSocketAddress getPredecessor(InetSocketAddress address) {
        rwl.readLock().lock();
        Map.Entry<String, InetSocketAddress> floor = consistentHashMap.lowerEntry(addressHash(address));
        if (floor == null) {
            floor = consistentHashMap.lastEntry();
        }
        rwl.readLock().unlock();
        return (floor == null) ? null : floor.getValue();
    }

    /**
     * remove a key value pair from the map.
     * @param addr The server address to remove
     */
    public void remove(InetSocketAddress addr) {
        rwl.writeLock().lock();
        consistentHashMap.remove(addressHash(addr));
        rwl.writeLock().unlock();
    }

    /**
     * Get keyrange arguments in the format start_hash,end_hash,IP:Port;start_hash,...
     * @return
     */
    public String getKeyrangeString() {
        rwl.readLock().lock();
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
        rwl.readLock().unlock();
        return items;
    }

    /**
     * Create a new instance from a string generated by {@link #getKeyrangeString()}.
     *
     * @param keyrange The formatted string containing a keyrange representation
     *
     * @return A new instance with the respective entries
     *
     * @throws IllegalArgumentException If the input string cannot be parsed as keyrange representation
     */
    public static ConsistentHashMap fromKeyrangeString(String keyrange)
            throws IllegalArgumentException {
        if (!keyrange.contains(";")) {
            throw new IllegalArgumentException("Bad format: No semicolon found");
        }

        String[] elements = keyrange.split(";");
        ConsistentHashMap newInstance = new ConsistentHashMap();
        InetSocketAddressTypeConverter converter = new InetSocketAddressTypeConverter();
        // process items
        for (String element : elements) {
            String[] elemParts = element.split(",");

            if (elemParts.length != 3) {
                throw new IllegalArgumentException(
                        "Bad format: expecting start_hash,end_hash,ip:port but got "
                        + element);
            }

            try {
                // only parse IP and add it, the hashes are checked later
                InetSocketAddress addr = converter.convert(elemParts[2]);
                newInstance.put(addr);
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not parse ip:port", e);
            }
        }

        // check if all input hashes and the order were correct by generating a string from the parsed result again
        String expectedInput = newInstance.getKeyrangeString();
        if (!keyrange.equals(expectedInput)) {
            throw new IllegalArgumentException("Given hash ranges do not match a consistent hash map ordering");
        }

        return newInstance;
    }

    public int size() {
        return consistentHashMap.size();
    }
}
