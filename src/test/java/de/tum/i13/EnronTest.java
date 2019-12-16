package de.tum.i13;

import de.tum.i13.client.KVLib;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

@Disabled
public class EnronTest {
    private static final Path TEST_DATA_PATH = Paths.get("../maildir");
    public static final Integer WAIT_TIME_BALANCE = 2000;

    public static final Integer kv1Port = 5161;
    public static final Integer kv2Port = 5162;
    public static final Integer kv3Port = 5163;
    public static final Integer kv4Port = 5164;
    public static final Integer kv5Port = 5165;
    public static final Integer ecsPort = 5160;

    @Test
    public void test(@TempDir Path tmpDir1, @TempDir Path tmpDir2, @TempDir Path tmpDir3,
                     @TempDir Path tmpDir4, @TempDir Path tmpDir5) throws InterruptedException, IOException, SocketCommunicatorException {
        Thread ecsThread = IntegrationTestHelpers.startECS(ecsPort);
        Thread kvThread1 = IntegrationTestHelpers.startKVServer(tmpDir1.toString(), kv1Port, ecsPort, 1);
//        Thread kvThread2 = IntegrationTestHelpers.startKVServer(tmpDir2.toString(), kv2Port, ecsPort, 1);
//        Thread kvThread3 = IntegrationTestHelpers.startKVServer(tmpDir3.toString(), kv3Port, ecsPort, 1);
//        Thread kvThread4 = IntegrationTestHelpers.startKVServer(tmpDir4.toString(), kv4Port, ecsPort, 1);
//        Thread kvThread5 = IntegrationTestHelpers.startKVServer(tmpDir5.toString(), kv5Port, ecsPort, 1);

        KVLib lib = new KVLib();
        lib.connect("127.0.0.1", kv1Port);

        Stream<Path> filePaths;
        try (Stream<Path> paths = Files.walk(TEST_DATA_PATH)) {
            filePaths = paths.limit(5000).filter(p -> !Files.isDirectory(p));

            filePaths.forEach(f -> {
                StringBuilder sb = new StringBuilder();
                try {
                    FileInputStream fis = new FileInputStream(f.toString());
                    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                    decoder.onMalformedInput(CodingErrorAction.IGNORE);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader br = new BufferedReader(isr);
                    String line = br.readLine();
                    while (line != null) {
                        sb.append(line);
                        line = br.readLine();
                    }
                    br.close();
                } catch (IOException e) {
                    fail(e);
                }
                String key = TEST_DATA_PATH
                        .relativize(f).toString().replaceAll("\\s+", "_");
                String value = sb.toString();
                lib.put(new KVItem(key, value));
            });
        }

        kvThread1.interrupt();
//        kvThread2.interrupt();
//        kvThread3.interrupt();
//        kvThread4.interrupt();
//        kvThread5.interrupt();

        kvThread1.join(WAIT_TIME_BALANCE);
//        kvThread2.join(WAIT_TIME_BALANCE);
//        kvThread3.join(WAIT_TIME_BALANCE);
//        kvThread4.join(WAIT_TIME_BALANCE);
//        kvThread5.join(WAIT_TIME_BALANCE);
        ecsThread.interrupt();
        ecsThread.join(WAIT_TIME_BALANCE);
    }
}
