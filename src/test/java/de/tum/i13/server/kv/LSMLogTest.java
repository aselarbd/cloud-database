package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LSMLogTest {

    @Test
    public void testAppend() {
        LSMLog wal = new LSMLog(null);

        assertEquals(0, wal.size());
        wal.append(new KVItem("key", "value"));
        assertEquals(1, wal.size());


    }
}