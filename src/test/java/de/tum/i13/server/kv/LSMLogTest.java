package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class LSMLogTest {

    private static final String TEST_DIR = "test-log";

    @AfterEach
    public void cleanup() throws IOException {
        Files.walk(Paths.get(TEST_DIR))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    public void testAppend() throws IOException {
        LSMLog wal = new LSMLog(Paths.get(TEST_DIR));

        assertEquals(0, wal.size());
        wal.append(new KVItem("key", "value"));
        assertEquals(1, wal.size());


    }
}