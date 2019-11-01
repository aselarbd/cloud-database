package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LRUCacheTest {

    private LRUCache lruCache = new LRUCache(5);

    @Test
    void get_basic() {
        KVItem KVPair = new KVItem("jbl","music speaker");
        lruCache.put(KVPair);
        assertEquals(KVPair,lruCache.get("jbl"));
    }

    @Test
    void put_basic() {
        KVItem KVPair_1 = new KVItem("jbl","music speaker");
        KVItem KVPair_2 = new KVItem("apple","mac books");
        lruCache.put(KVPair_1);
        lruCache.put(KVPair_2);
        assertEquals(KVPair_2,lruCache.get(KVPair_2.getKey()));
        assertNotEquals(KVPair_1,lruCache.get(KVPair_2.getKey()));
    }

    @Test
    void delete_basic() {
        KVItem KVPair = new KVItem("apple","mac books");
        lruCache.put(KVPair);
        lruCache.delete(KVPair);
        assertNull(lruCache.get(KVPair.getKey()));
    }


    @Test
    void testCache () {
        KVItem KV_1 = new KVItem("1","1");
        KVItem KV_2 = new KVItem("2","3");
        KVItem KV_3 = new KVItem("3","4");
        KVItem KV_4 = new KVItem("4","7");
        KVItem KV_5 = new KVItem("6","10");
        KVItem KV_6 = new KVItem("1","5");
        KVItem KV_7 = new KVItem("12","7");
        KVItem KV_8 = new KVItem("5","2");

        lruCache.put(KV_1);
        lruCache.put(KV_2);
        lruCache.put(KV_3);
        lruCache.put(KV_4);
        lruCache.put(KV_5);
        assertEquals(KV_1,lruCache.get("1"));
        assertEquals(KV_3, lruCache.get("3"));
        lruCache.put(KV_6);
        assertEquals(KV_6,lruCache.get("1"));
        lruCache.put(KV_7);
        lruCache.put(KV_8);
        assertNull(lruCache.get("4"));
    }
}