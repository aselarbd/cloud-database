package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LSMStore implements KVStore {

    private LSMLog lsmLog;
    private LSMCache lsmCache;
    private Path lsmFileDir;

    private LSMFlusher lsmFlusher;

    public LSMStore(Path dataDir) throws IOException {

        Path lsmLogFileDir = Paths.get(dataDir.toString(), "log");
        this.lsmFileDir = Paths.get(dataDir.toString(), "data");

        this.lsmLog = new LSMLog(lsmLogFileDir);
        TreeMap<String, KVItem> log = lsmLog.readAllSinceFlush();

        this.lsmCache = new LSMCache();

        log.forEach((s, i) -> lsmCache.put(i));

        lsmFlusher = new LSMFlusher(lsmCache, lsmFileDir, lsmLog);
        lsmFlusher.start();
    }

    private List<LSMFile> listLSMFiles(Path lsmFileDir) throws IOException {
        List<LSMFile> lsmFiles = new ArrayList<>();
        List<Path> files = Files.list(lsmFileDir).collect(Collectors.toList());
        for (Path f : files) {
            lsmFiles.add(new LSMFile(f.getParent(), f.getFileName().toString()));
        }
        return lsmFiles;
    }

    @Override
    public String put(KVItem item) throws IOException {
        if (item.getTimestamp() == 0) {
            item.setTimestamp(Instant.now().toEpochMilli());
        }
        lsmLog.append(item);
        return lsmCache.put(item);
    }

    private static class Lookup {

        private LSMFile lsmFile;
        private long position;

        private Lookup(LSMFile lsmFile, long position) {
            this.lsmFile = lsmFile;
            this.position = position;
        }
    }

    // TODO: Merge cache and lsmFiles and get latest item
    @Override
    public String get(String key) throws IOException {

        KVItem cachedItem = lsmCache.get(key);
        if (cachedItem != null) {
            return cachedItem.getValue();
        }

        ArrayList<Lookup> lookUps = new ArrayList<>();
        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);

        for (LSMFile f : lsmFiles) {
            Long position = f.readIndex().get(key);
            if (position != null) {
                lookUps.add(new Lookup(f, position));
            }
        }

        TreeMap<Long, KVItem> items = new TreeMap<>();
        for (Lookup l : lookUps) {
            KVItem kvItem = l.lsmFile.readValue(l.position);
            if (kvItem != null) {
                items.put(kvItem.getTimestamp(), kvItem);
            }
        }

        if (items.size() > 0) {
            String result = items.lastEntry().getValue().getValue();
            if (!result.equals(Constants.DELETE_MARKER)) {
                return result;
            }
        }

        return null;
    }
}
