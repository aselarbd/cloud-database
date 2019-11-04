package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 *  LRUCache class contains the implementation of the LRU cache. In here cache is maintain in a hashtable and
 *  least recently used index is maintained in a doubled linklist
 */
public class LRUCache implements KVCache {

    private int size;
    private int currentNoOfElements;
    private HashMap<String,KVItem> cache;
    private LinkedList <String> lru;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    private Logger logger = Logger.getLogger(LRUCache.class.getName());

    public LRUCache(int size){
        this.size = size;
        this.currentNoOfElements = 0;
        this.cache= new HashMap<String,KVItem >();
        this.lru = new LinkedList <String>();
        logger.info("Cache is initialized to LRU cache");
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

            if(this.cache.containsKey(key)) {
                this.lru.remove(key);
                this.lru.addFirst(key);
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
        if (size <= 0) {
            return;
        }
        try {
            rwl.writeLock().lock();

            String key = item.getKey();
            if (this.cache.containsKey(key)){
                this.lru.remove(key);
                this.lru.addFirst(key);
                this.cache.replace(key,item);
            }else{
                if (this.currentNoOfElements < this.size){
                    this.currentNoOfElements ++;
                    this.lru.addFirst(key);
                    this.cache.put(key,item);
                }else {
                    String replaceKey =  this.lru.removeLast();
                    this.cache.remove(replaceKey);
                    this.lru.addFirst(key);
                    this.cache.put(key,item);
                }
            }
            logger.info("new KVItem added to the cache");
        } finally {
            rwl.writeLock().unlock();
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
                this.lru.remove(key);
                this.cache.remove(key);
                logger.info("KVItem is deleted from the cache");
            }
        }finally {
            rwl.writeLock().unlock();
        }
    }
}
