package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LSMStore implements KVStore {

    private LSMLog lsmLog;
    private LSMCache lsmCache;
    private List<Path> lsmFiles = new ArrayList<>();

    private LSMFlusher lsmFlusher;

    public LSMStore(Path lsmLogFileDir, Path lsmFileDir) throws IOException {

        this.lsmLog = new LSMLog(lsmLogFileDir);

        listLSMFiles(lsmFileDir);

        this.lsmCache = new LSMCache();
        lsmFlusher = new LSMFlusher(lsmCache, lsmFileDir);
        lsmFlusher.start();
    }

    private void listLSMFiles(Path lsmFileDir) throws IOException {
        List<Path> files = Files.list(lsmFileDir).collect(Collectors.toList());
        for (Path p : files) {
            lsmFiles.add(p);
        }
    }

    @Override
    public String put(KVItem item) {
        lsmLog.append(item);
        return lsmCache.put(item);
    }

    // TODO: Merge cache and lsmFiles and get latest item
    @Override
    public String get(String key) throws IOException {
        return null;
    }
}
