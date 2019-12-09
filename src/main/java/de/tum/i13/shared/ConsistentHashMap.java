package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
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
    private TreeMap<String, List<InetSocketAddress>> consistentHashMap = new TreeMap<>();
    private TreeMap<String, List<String>> replicaOfMapping = new TreeMap<>();
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

    private List<InetSocketAddress> getSuccessorList(String key) {
        Map.Entry<String, List<InetSocketAddress>> ceiling = consistentHashMap.ceilingEntry(getMD5DigestHEX(key));
        if (ceiling == null) {
            ceiling = consistentHashMap.firstEntry();
        }
        // again perform a null check as map might be empty
        return (ceiling == null) ? new ArrayList<>() : ceiling.getValue();
    }

    /**
     * Save a server entry in the HashMap. The key will be the HEX-String of the hash
     * of the given IP:Port string.
     *
     * @param addr The IP:Port value as InetSocketAddress
     */
    public void put(InetSocketAddress addr) {
        rwl.writeLock().lock();
        ArrayList<InetSocketAddress> items = new ArrayList<>();
        items.add(addr);
        consistentHashMap.put(addressHash(addr), items);
        rwl.writeLock().unlock();
    }

    public void putReplica(InetSocketAddress baseAddr, InetSocketAddress replica) {
        rwl.writeLock().lock();
        final String addrHash = addressHash(baseAddr);
        List<InetSocketAddress> items = consistentHashMap.get(addrHash);
        if (items != null) {
            items.add(replica);
            // remember all nodes which are replicated to speed up deletions
            List<String> cache = replicaOfMapping.get(replica);
            if (cache == null) {
                cache = new ArrayList<>();
                replicaOfMapping.put(addressHash(replica), cache);
            }
            cache.add(addrHash);
        }
        rwl.writeLock().unlock();
    }

    /**
     * Get a key from the map given a plaintext key. The key will be hashed.
     * Afterwards this function returns value, which is stored under the
     * lexicographically next (hash-)key. If no such key exists, the first
     * available key is used.
     *
     * @param key plain text key
     * @return The server address saved under the key, which lexicographically next to
     * the hashed key or the first, if no such key exists, or null if the map
     * is empty.
     */
    public InetSocketAddress getSuccessor(String key) {
        rwl.readLock().lock();
        List<InetSocketAddress> allItems = getSuccessorList(key);
        InetSocketAddress result = (allItems == null || allItems.isEmpty()) ? null : allItems.get(0);
        rwl.readLock().unlock();
        return result;
    }

    public InetSocketAddress getSuccessor(InetSocketAddress key) {
        if (key != null) {
            return getSuccessor(addressHash(key));
        }
        return null;
    }

    /**
     * Gets all addresses responsible for the lexicographically next hash of key.
     * If no such hash exists, the first available hash is used.
     *
     * @param key plain text key
     * @return All server addresses for the given key, either lexicographically next
     *  or the first. If the map is empty, null is returned.
     */
    public List<InetSocketAddress> getAllSuccessors(String key) {
        rwl.readLock().lock();
        List<InetSocketAddress> allItems = getSuccessorList(key);
        // ensure a copy of the list, as caller might modify it
        List<InetSocketAddress> elements = new ArrayList<>(allItems);
        rwl.readLock().unlock();
        return elements;
    }

    public List<InetSocketAddress> getAllSuccessors(InetSocketAddress key) {
        if (key != null) {
            return getAllSuccessors(addressHash(key));
        }
        return new ArrayList<>();
    }

    public InetSocketAddress getPredecessor(InetSocketAddress address) {
        rwl.readLock().lock();
        Map.Entry<String, List<InetSocketAddress>> floor = consistentHashMap.lowerEntry(addressHash(address));
        if (floor == null) {
            floor = consistentHashMap.lastEntry();
        }
        rwl.readLock().unlock();
        return (floor == null) ? null : floor.getValue().get(0);
    }

    /**
     * remove a key value pair from the map.
     * @param addr The server address to remove
     */
    public void remove(InetSocketAddress addr) {
        rwl.writeLock().lock();
        String addrHash = addressHash(addr);
        if (consistentHashMap.containsKey(addrHash)) {
            List<InetSocketAddress> items = consistentHashMap.get(addrHash);
            items.remove(addr);
            // remove everything if list got empty
            if (items.isEmpty()) {
                consistentHashMap.remove(addrHash);
            }
        }
        // delete this address from all elements it is a replica of
        if (replicaOfMapping.containsKey(addrHash)) {
            List<String> replicaOf = replicaOfMapping.get(addr);
            for (String hash : replicaOf) {
                List<InetSocketAddress> otherKeyServers = consistentHashMap.get(hash);
                otherKeyServers.remove(addr);
                // replica are never the only element, so list removal is not necessary here
            }
        }
        rwl.writeLock().unlock();
    }

    private String buildKeyrange(String startHash, String endHash, List<InetSocketAddress> ipList) {
        String items = "";
        for (InetSocketAddress addr : ipList) {
            String ipPort = InetSocketAddressTypeConverter.addrString(addr);
            items += startHash + "," + endHash + "," + ipPort + ";";
        }
        return items;
    }

    /**
     * Get keyrange arguments in the format start_hash,end_hash,IP:Port;start_hash,...
     * @return
     */
    public String getKeyrangeString() {
        rwl.readLock().lock();
        if (consistentHashMap.isEmpty()) {
            return "";
        }
        String items = "";
        String startHash = "";
        String endHash = "";
        List<InetSocketAddress> ipList = null;
        for (Map.Entry<String, List<InetSocketAddress>> entry : consistentHashMap.entrySet()) {
            // current hash is end hash for previous one
            endHash = entry.getKey();
            if (ipList != null) {
                items += buildKeyrange(startHash, endHash, ipList);
            }
            // prepare items to be written in the next iteration
            startHash = entry.getKey();
            ipList = entry.getValue();
        }
        // process the last item - it has the first item's hash as end hash
        endHash = consistentHashMap.firstKey();
        if (ipList != null) {
            items += buildKeyrange(startHash, endHash, ipList);
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
