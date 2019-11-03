package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LSMStore implements KVStore {

    private LSMLog lsmLog;
    private Path lsmFileDir;
    private LSMCache lsmCache;

    private LSMFlusher lsmFlusher;

    public LSMStore(Path lsmLogFileDir, Path lsmFileDir) throws IOException {

        this.lsmLog = new LSMLog(lsmLogFileDir);
        this.lsmFileDir = lsmFileDir;

        this.lsmCache = new LSMCache();
        lsmFlusher = new LSMFlusher(lsmCache, lsmFileDir);
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
    public String put(KVItem item) {
        lsmLog.append(item);
        return lsmCache.put(item);
    }

    private class Lookup {

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
            String result = items.firstEntry().getValue().getValue();
            if (!result.equals(Constants.DELETE_MARKER)) {
                return result;
            }
        }

        return null;
    }
}
