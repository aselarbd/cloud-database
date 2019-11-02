package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LFUCacheTest {

    private LFUCache lfuCache = new LFUCache(3);

    @Test
    void get_basic() {
        KVItem KVPair = new KVItem("jbl","music speaker");
        lfuCache.put(KVPair);
        assertEquals(KVPair,lfuCache.get("jbl"));
    }

    @Test
    void put_basic() {
        KVItem KVPair_1 = new KVItem("jbl","music speaker");
        KVItem KVPair_2 = new KVItem("apple","mac books");
        lfuCache.put(KVPair_1);
        lfuCache.put(KVPair_2);
        assertEquals(KVPair_2,lfuCache.get(KVPair_2.getKey()));
        assertNotEquals(KVPair_1,lfuCache.get(KVPair_2.getKey()));
    }

    @Test
    void delete_basic() {
        KVItem KVPair = new KVItem("apple","mac books");
        lfuCache.put(KVPair);
        assertEquals(KVPair,lfuCache.get("apple"));
        lfuCache.delete(KVPair);
        assertNull(lfuCache.get(KVPair.getKey()));
    }

    @Test
    void testCache(){
        KVItem KV_1 = new KVItem("1","1");
        KVItem KV_2 = new KVItem("2","2");
        KVItem KV_3 = new KVItem("3","3");
        KVItem KV_4 = new KVItem("4","4");
        KVItem KV_5 = new KVItem("5","5");
        KVItem KV_6 = new KVItem("6","6");
        KVItem KV_7 = new KVItem("7","7");
        KVItem KV_8 = new KVItem("8","8");

        lfuCache.put(KV_1);
        lfuCache.put(KV_2);
        assertEquals(KV_2,lfuCache.get("2"));
        lfuCache.put(KV_3);
        lfuCache.put(KV_4);
        assertNull(lfuCache.get("1"));
        lfuCache.put(KV_5);
        assertNull(lfuCache.get("3"));
        assertEquals(KV_4,lfuCache.get("4"));
        assertEquals(KV_5,lfuCache.get("5"));
        lfuCache.put(KV_6);
        assertNull(lfuCache.get("2"));
        assertEquals(KV_6, lfuCache.get("6"));
        assertEquals(KV_4,lfuCache.get("4"));
        lfuCache.put(KV_7);
        assertNull(lfuCache.get("5"));
        lfuCache.put(KV_8);
        assertNull(lfuCache.get("7"));
        assertEquals(KV_6,lfuCache.get("6"));
        assertEquals(KV_8,lfuCache.get("8"));
        assertEquals(KV_8,lfuCache.get("8"));

    }

}