package de.tum.i13.server.kv.caches;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FIFOCacheTest {

    private final FIFOCache fifoCache = new FIFOCache(3);

    @Test
    void get_basic() {
        KVItem KV = new KVItem("jbl", "music box");
        fifoCache.put(KV);
        assertEquals(KV,fifoCache.get(KV.getKey()));
    }

    @Test
    void put_basic() {
        KVItem KV_1 = new KVItem("jbl", "music box");
        KVItem KV_2 = new KVItem("apple", "mac books");
        fifoCache.put(KV_1);
        fifoCache.put(KV_2);
        assertEquals(KV_1,fifoCache.get(KV_1.getKey()));
        assertNotEquals(KV_1, fifoCache.get(KV_2.getKey()));
    }

    @Test
    void delete_basic() {
        KVItem KV_1 = new KVItem("jbl", "music box");
        KVItem KV_2 = new KVItem("apple", "mac books");
        fifoCache.put(KV_1);
        fifoCache.put(KV_2);
        assertEquals(KV_1,fifoCache.get(KV_1.getKey()));
        assertEquals(KV_2, fifoCache.get(KV_2.getKey()));
        fifoCache.delete(KV_1.getKey());
        assertNull(fifoCache.get(KV_1.getKey()));
        fifoCache.delete(KV_2.getKey());
        assertNull(fifoCache.get(KV_2.getKey()));
    }

    @Test
    void fifoTest(){
        KVItem KV_1 = new KVItem("1", "1");
        KVItem KV_2 = new KVItem("2", "2");
        KVItem KV_3 = new KVItem("3", "3");
        KVItem KV_4 = new KVItem("4", "4");
        KVItem KV_4_1 = new KVItem("4", "4_1");
        KVItem KV_5 = new KVItem("5", "5");
        KVItem KV_6 = new KVItem("6", "6");
        KVItem KV_7 = new KVItem("7", "7");

        fifoCache.put(KV_1);
        fifoCache.put(KV_2);
        fifoCache.put(KV_3);
        assertEquals(KV_1,fifoCache.get(KV_1.getKey()));
        assertEquals(KV_2,fifoCache.get(KV_2.getKey()));
        assertEquals(KV_3,fifoCache.get(KV_3.getKey()));
        fifoCache.put(KV_4);
        assertEquals(KV_2, fifoCache.get(KV_2.getKey()));
        assertEquals(KV_3, fifoCache.get(KV_3.getKey()));
        assertEquals(KV_4, fifoCache.get(KV_4.getKey()));
        assertNull(fifoCache.get(KV_1.getKey()));
        fifoCache.put(KV_5);
        fifoCache.put(KV_6);
        assertNull(fifoCache.get(KV_2.getKey()));
        assertNull(fifoCache.get(KV_3.getKey()));
        fifoCache.put(KV_4_1);
        assertEquals(KV_4_1,fifoCache.get(KV_4.getKey()));
        fifoCache.put(KV_7);
    }
}