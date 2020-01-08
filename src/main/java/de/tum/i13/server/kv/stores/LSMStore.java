package de.tum.i13.server.kv.stores;

import de.tum.i13.server.kv.KVStore;
import de.tum.i13.lsm.LSMCache;
import de.tum.i13.lsm.LSMFile;
import de.tum.i13.lsm.LSMFlusher;
import de.tum.i13.lsm.LSMLog;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LSMStore provides a KVStore implementation using an LSM-tree
 * The LSM-tree is inspired by the implementations in BigTable and HBase.
 *
 * {@link LSMCache} is used as in in-memory store to
 * which new data is written. A separate worker thread running
 * {@link LSMFlusher} stores the cache in a new {@link LSMFile}. To prevent
 * losing items on server crashes we write every change to {@link LSMLog}
 * before saving it in the cache and try to recover the cache from that log
 * on restart.
 */
public class LSMStore implements KVStore {

    private LSMLog lsmLog;
    private LSMCache lsmCache;
    private final Path lsmFileDir;

    /**
     * create a new LSMStore and save all data in the given directory
     *
     * @param dataDir Path to the directory where data will be stored.
     *
     * @throws IOException If there's some error accessing the given directory
     */
    public LSMStore(Path dataDir) throws IOException {

        Path lsmLogFileDir = Paths.get(dataDir.toString(), "log");
        this.lsmFileDir = Paths.get(dataDir.toString(), "data");
        // create subdirectory if it doesn't exist
        if (!lsmFileDir.toFile().exists()) {
            lsmFileDir.toFile().mkdir();
        }

        this.lsmLog = new LSMLog(lsmLogFileDir);
        TreeMap<String, KVItem> log = lsmLog.readAllSinceFlush();

        this.lsmCache = new LSMCache();

        log.forEach((s, i) -> lsmCache.put(i));

        LSMFlusher lsmFlusher = new LSMFlusher(lsmCache, lsmFileDir, lsmLog);
        lsmFlusher.start();
    }

    /**
     * list all LSMFiles in a given directory
     *
     * @param lsmFileDir directory to look for LSMFiles
     *
     * @return a list of all LSMFiles in that directory
     * @throws IOException
     */
    private List<LSMFile> listLSMFiles(Path lsmFileDir) throws IOException {
        List<LSMFile> lsmFiles = new ArrayList<>();
        try (Stream<Path> paths = Files.list(lsmFileDir)) {
            List<Path> files = paths.collect(Collectors.toList());
            for (Path f : files) {
                lsmFiles.add(new LSMFile(f.getParent(), f.getFileName().toString()));
            }
        }
        return lsmFiles;
    }

    /**
     * put a new item to the LSMStore
     *
     * @param item item to store or update
     *
     * @return "success" if the item was added, "update" if it was already present before
     *
     * @throws IOException if there's a problem writing to the LSMLog
     */
    @Override
    public String put(KVItem item) throws IOException {
        if (item.getTimestamp() == 0) {
            item.setTimestamp(Instant.now().toEpochMilli());
        }
        String result = "success";
        if (get(item.getKey()) != null) {
            result = "update";
        }
        lsmLog.append(item);
        lsmCache.put(item);
        return result;
    }

    /**
     * helper class to lookup the position of a KVItem in an LSMFile
     */
    private static class Lookup {

        private final LSMFile lsmFile;
        private final long position;

        private Lookup(LSMFile lsmFile, long position) {
            this.lsmFile = lsmFile;
            this.position = position;
        }
    }

    /**
     * get tries to get an item from the LSMTree by first looking if it is
     * in the LSMCache and if it is not querying all LSMFiles and finding the
     * most recent version.
     *
     * @param key key of the requested item
     *
     * @return the requested item or null if it does not exist
     *
     * @throws IOException if an IO error occurs on reading the LSMFiles
     */
    @Override
    public KVItem get(String key) throws IOException {

        KVItem cachedItem = lsmCache.get(key);
        if (cachedItem != null) {
            if (!cachedItem.getValue().equals(Constants.DELETE_MARKER)) {
                return cachedItem;
            }
        }

        ArrayList<Lookup> lookUps = new ArrayList<>();
        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);

        for (LSMFile f : lsmFiles) {
            Long position = f.readIndex().get(key);
            if (position != null) {
                lookUps.add(new Lookup(f, position));
            } else {
                f.close();
            }
        }

        TreeMap<Long, KVItem> items = new TreeMap<>();
        for (Lookup l : lookUps) {
            KVItem kvItem = l.lsmFile.readValue(l.position);
            if (kvItem != null) {
                items.put(kvItem.getTimestamp(), kvItem);
            }
            l.lsmFile.close();
        }

        if (items.size() > 0) {
            KVItem result = items.lastEntry().getValue();
            if (!result.getValue().equals(Constants.DELETE_MARKER)) {
                return result;
            }
        }

        return null;
    }

    @Override
    public Set<String> getAllKeys(Predicate<String> predicate) throws IOException {

        TreeMap<String, KVItem> cacheSnapshot = lsmCache.getShallowLsmCopy();

        Set<String> matchingKeys = cacheSnapshot.keySet()
                .stream()
                .filter(predicate)
                .collect(Collectors.toSet());

        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);

        for (LSMFile f : lsmFiles) {
            TreeMap<String, Long> node = f.readIndex();
            matchingKeys.addAll(
                    node.keySet()
                            .stream()
                            .filter(predicate)
                            .collect(Collectors.toSet())
            );
            f.close();
        }

        return matchingKeys;
    }

    /**
     * scan tries to get partially matched item set from the LSMTree by first looking if it is
     * in the LSMCache and if it is not querying all LSMFiles and finding the
     * most recent version.
     *
     * @param key partial key
     *
     * @return the requested item or empty set if it does not exist
     *
     * @throws IOException if an IO error occurs on reading the LSMFiles
     */
    @Override
    public Set<KVItem> scan (String key) throws IOException{
        Set<KVItem> cachedSet = lsmCache.scan(key);
        Set<KVItem> totalSet = new HashSet<>(cachedSet);

        Set<KVItem> lsmSet = new HashSet<>();

        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);
        Set<String> totalKeySet =  new HashSet<>();

        for (LSMFile f: lsmFiles){
           totalKeySet.addAll(f.readIndex().keySet());
        }
        ArrayList<Lookup> lookUp = new ArrayList<>();
        for (String k : totalKeySet){
            lookUp.clear();
            for (LSMFile f: lsmFiles){
                Long position = f.readIndex().get(k);
                if (position != null) {
                    lookUp.add(new Lookup(f, position));
                } else {
                    f.close();
                }
            }
            TreeMap<Long, KVItem> items = new TreeMap<>();
            for (Lookup l : lookUp) {
                KVItem kvItem = l.lsmFile.readValue(l.position);
                if (kvItem != null) {
                    items.put(kvItem.getTimestamp(), kvItem);
                }
                l.lsmFile.close();
            }
            if (items.size() > 0) {
                KVItem lastEntry = items.lastEntry().getValue();
                if (!lastEntry.getValue().equals(Constants.DELETE_MARKER)) {
                    lsmSet.add(lastEntry);
                }
            }
        }
        totalSet.addAll(lsmSet);
        return totalSet;
    }
}
