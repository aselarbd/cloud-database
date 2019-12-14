package de.tum.i13.lsm;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Comparator;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class LSMFileTest {

    private static final String TEST_DIR = "datatest";

    @AfterEach
    public void afterEach() throws IOException {
        Files.walk(Paths.get(TEST_DIR))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    public void testGetName() throws IOException {
        Files.createDirectories(Paths.get(TEST_DIR, "testName"));
        Files.createFile(Paths.get(TEST_DIR, "testName/data-testName"));
        Files.createFile(Paths.get(TEST_DIR, "testName/index-testName"));

        try (LSMFile lsmFile = new LSMFile(Paths.get(TEST_DIR), "testName")) {
            assertEquals("testName", lsmFile.getName());
        }
    }

    @Test
    public void testReadLSMFile() throws IOException {
        LSMFile lsmFile = new LSMFile(Paths.get(TEST_DIR));
        String name = lsmFile.getName();
        KVItem kvItem1 = new KVItem("key", "value", Instant.now().toEpochMilli());
        KVItem kvItem2 = new KVItem("key2", "value2", Instant.now().toEpochMilli());
        lsmFile.append(kvItem1);
        lsmFile.append(kvItem2);
        lsmFile.close();

        lsmFile = new LSMFile(Paths.get(TEST_DIR), name);
        TreeMap<String, Long> index = lsmFile.readIndex();
        assertEquals(kvItem1.getKey(), index.firstKey());
        assertEquals(0, index.get(kvItem1.getKey()));

        assertEquals(kvItem2.getKey(), index.lastKey());
        assertEquals(49, index.get(kvItem2.getKey()));

        KVItem receivedItem1 = lsmFile.readValue(index.firstEntry().getValue());
        assertEquals(kvItem1.getKey(), receivedItem1.getKey());
        assertEquals(kvItem1.getValue(), receivedItem1.getValue());
        assertEquals(kvItem1.getTimestamp(), receivedItem1.getTimestamp());

        KVItem receivedItem2 = lsmFile.readValue(index.firstEntry().getValue());
        assertEquals(kvItem2.getKey(), receivedItem2.getKey());
        assertEquals(kvItem2.getValue(), receivedItem2.getValue());
        assertEquals(kvItem2.getTimestamp(), receivedItem2.getTimestamp());
    }

}