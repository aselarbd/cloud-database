package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * LFUCache class contains implementation of the LFU Cache. In here cache maintain in a hash map and frequency of each
 * key is also maintain in a hash table. Also, different levels of frequency is maintained in a hash map and
 * linked hash set.
 */
public class LFUCache implements KVCache {

    private int size;
    private int currentNoOfElements;
    private int minFrequency = 0;
    private HashMap <String,KVItem> cache;
    private HashMap <String, Integer> frequency;
    private HashMap <Integer, LinkedHashSet<String>> frequencyLists;


    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    private Logger logger = Logger.getLogger(LFUCache.class.getName());

    public LFUCache (int size) {
        this.size = size;
        this.currentNoOfElements = 0;
        this.cache = new HashMap<>();
        this.frequency = new HashMap<>();
        this.frequencyLists = new HashMap<>();
        logger.info("Cache is initialized to LFU cache");
    }

    /**
     * get method used to get a KVItem from a cache
     * @param key : key of the required item
     * @return : KVItem object of requested key
     */
    @Override
    public KVItem get(String key) {

        try {
            rwl.readLock().lock();

            if (this.cache.containsKey(key)){
                increaseFrequency(key);
                logger.info("KVItem is found the cache for <key> : "+key);
                return this.cache.get(key);
            }
        }finally {
            rwl.readLock().unlock();
        }
        logger.info("Requested item is not found in the cache");
        return null;
    }

    /**
     * put method update the cache
     * @param item : KVItem that need to put to the cache
     */
    @Override
    public void put(KVItem item) {
        try {
            rwl.writeLock().lock();

            String key = item.getKey();
            if (this.cache.containsKey(key)){
                increaseFrequency(key);
                this.cache.replace(key,item);
            }else{
                if (this.currentNoOfElements < this.size){
                    this.currentNoOfElements ++;
                    increaseFrequency(key);
                    this.cache.put(key,item);
                }else {
                    String replaceKey = this.frequencyLists.get(this.minFrequency).iterator().next();
                    this.frequencyLists.get(this.minFrequency).remove(replaceKey);
                    this.frequency.remove(replaceKey);
                    this.cache.remove(replaceKey);
                    increaseFrequency(item.getKey());
                    this.cache.put(key,item);
                }
            }
            logger.info("new KVItem added to the cache");
        }finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * increaseFrequency function maintain the frequency lists.
     * @param key : newly added or accessed key need to increase the frequency
     */
    private void increaseFrequency(String key) {
        int currentFrequency = 0;

        if (this.frequency.containsKey(key)) {
            currentFrequency = this.frequency.get(key);
        }else {
            this.minFrequency = 1;
        }

        this.frequency.put(key,currentFrequency+1);

        if (currentFrequency !=0) {
            this.frequencyLists.get(currentFrequency).remove(key);
            if(this.frequencyLists.get(currentFrequency).isEmpty()){
                this.frequencyLists.remove(currentFrequency);
                this.minFrequency++;
            }
        }

        if (this.frequencyLists.containsKey(currentFrequency+1)){
            this.frequencyLists.get(currentFrequency+1).add(key);
        }else {
            this.frequencyLists.put(currentFrequency+1, new LinkedHashSet<>());
            this.frequencyLists.get(currentFrequency+1).add(key);
        }
    }

    /**
     * delete method deletes given item from the cache
     * @param item : KVItem object that need to delete from the cache
     */
    @Override
    public void delete(KVItem item) {
        try {
            rwl.writeLock().lock();

            String key = item.getKey();
            if (this.cache.containsKey(key)){
                this.currentNoOfElements --;
                this.frequencyLists.get(this.frequency.get(key)).remove(key);
                this.frequency.remove(key);
                this.cache.remove(key);
                logger.info("KVItem is deleted from the cache");
            }

        }finally {
            rwl.writeLock().unlock();
        }
    }
}
