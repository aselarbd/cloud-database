package de.tum.i13;

import de.tum.i13.client.KVLib;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

public class BenchmarkTest {

    private static final Path TEST_DATA_PATH = Paths.get("../maildir");
    private static final String CACHE_SIZE = "200";
    private static final String CACHE_DISPLACEMENT = "LRU";
    private static final String ECS_HOST = "localhost";
    private static final String ECS_PORT = "5050";
    private static final String ECS_ADDRESS = ECS_HOST + ":" + ECS_PORT;
    private static final int START_PORT = 5150;
    private static final String DATA_DIR = "data/kv";
    private static final String LOGLEVEL = "INFO";


    private Process ecs;
    private Process[] kvServers;

    @Disabled
    @Test
    public void testAddAllEnronMails() throws IOException {
        KVLib[] conns = connectClients(20);

        try (Stream<Path> s = Files.walk(TEST_DATA_PATH)) {
            s.parallel().forEach((f) -> {
                if (Files.isDirectory(f)) {
                    return;
                }

                StringBuilder sb = new StringBuilder();
                try {
                    FileInputStream fis = new FileInputStream(f.toString());
                    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
                    decoder.onMalformedInput(CodingErrorAction.IGNORE);
                    InputStreamReader isr = new InputStreamReader(fis);
                    BufferedReader br = new BufferedReader(isr);
                    String line = br.readLine();
                    while(line != null) {
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

                System.out.println("put " + key + " " + value);
                Random random = new Random();
                int conn =  random.nextInt(conns.length);
                synchronized (conns[conn]) {
                    conns[conn].put(new KVItem(key, value));
                }
            });
        }
    }

    private KVLib[] connectClients(int count) {
        KVLib[] connections = new KVLib[count];
        for (int i = 0; i < connections.length; i++) {
            try {
                KVLib lib = new KVLib();
                lib.connect("localhost", 5150);
                connections[i] = lib;
            } catch (SocketCommunicatorException e) {
                fail(e);
            }
        }
        return connections;
    }

    private void resetDataDir() throws IOException {
        if (Files.exists(Paths.get(DATA_DIR).getParent())) {
            Files.walk(Paths.get(DATA_DIR).getParent())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        Files.createDirectory(Paths.get(DATA_DIR).getParent());
    }

    void runServers(int count) throws IOException, InterruptedException {
        resetDataDir();

        ProcessBuilder serverRunner = new ProcessBuilder();
        serverRunner.command(
                "java",
                "-jar",
                "target/ecs-server.jar",
                "-p",
                "5050",
                "-ll=" + LOGLEVEL
        );
        serverRunner.inheritIO();
        ecs = serverRunner.start();

        kvServers = new Process[count];
        for (int i = 0; i < kvServers.length; i++) {
            ProcessBuilder kvs = new ProcessBuilder();
            kvs.command(
                    "java", "-jar", "target/kv-server.jar",
                    "-c ", CACHE_SIZE,
                    "-s=" + CACHE_DISPLACEMENT,
                    "-b ", ECS_ADDRESS,
                    "-p ", "" + (START_PORT + i),
                    "-d ", DATA_DIR + i,
                    "-l ", "" + i + ".kvserver.log",
                    "-ll=" + LOGLEVEL
            );
            kvs.inheritIO();
            kvServers[i] = kvs.start();
        }
        Thread.sleep(10000);
    }

    public void stopServers() throws InterruptedException, IOException {
        for (int i = 0; i < kvServers.length; i++) {
            kvServers[i].destroy();
        }
        ecs.destroy();

        Thread.sleep(5000);
        ecs.destroy();
        for (int i = 0; i < kvServers.length; i++) {
            kvServers[i].destroyForcibly();
        }

        Thread.sleep(5000);
        resetDataDir();
    }

    public void benchmark(int[] serverCounts, int[] clientCounts) throws IOException, InterruptedException {

        Map<Integer, HashMap<Integer, Double>> results = new HashMap<>();
        for (int serverCount : serverCounts) {
            runServers(serverCount);
            HashMap<Integer, Double> benchmarkWithXClients = benchmarkWithXClients(clientCounts);
            results.put(serverCount, benchmarkWithXClients);
            stopServers();
        }

        results.forEach((s, r) -> r.forEach((c, a) -> {
            System.out.printf("average latency for %s servers and %s clients: %s%n", s, c, a);
        }));
    }

    HashMap<Integer, Double> benchmarkWithXClients(int[] clientCounts) throws IOException {

        HashMap<Integer, Double> clientsToLatencyAvg = new HashMap<>();

        for (int i = 0; i < clientCounts.length; i++) {
            KVLib[] connections = connectClients(5);

            List<Long> execTimes = new ArrayList<>();

            Stream<Path> filePaths;
            try (Stream<Path> paths = Files.walk(TEST_DATA_PATH)) {
                filePaths = paths.limit(5000).filter(p -> !Files.isDirectory(p));

                filePaths.parallel().forEach(f -> {
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

                    Random random = new Random();
                    int conn = random.nextInt(connections.length);

                    Instant startTime;
                    Instant stopTime;
                    synchronized (connections[conn]) {
                        startTime = Instant.now();

                        connections[conn].put(new KVItem(key, value));

                        stopTime = Instant.now();
                    }

                    long duration = Duration.between(startTime, stopTime).toMillis();
                    execTimes.add(duration);
                });

                Long sum = execTimes.stream().reduce((long) 0, Long::sum);
                double avg = (double) sum / execTimes.size();

                clientsToLatencyAvg.put(clientCounts[i], avg);
            }
        }

        return clientsToLatencyAvg;
    }

    @Disabled
    @Test
    public void benchmarkAll() throws IOException, InterruptedException {
        benchmark(new int[]{1, 5, 10, 15, 20}, new int[]{1, 5, 10, 15, 20});
    }
}
