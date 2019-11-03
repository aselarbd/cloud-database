package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMFlusher extends Thread {

    private static final long CACHE_FLUSH_FREQUENCY = 5000;
    private static final int MIN_FLUSH_SIZE = 3;
    private LSMCache lsmCache;
    private Path lsmFileDir;

    private boolean shutDown = false;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    public LSMFlusher(LSMCache lsmCache, Path lsmFileDir) {

        this.lsmCache = lsmCache;
        this.lsmFileDir = lsmFileDir;
    }

    // TODO: Does this makes sense?
    public void setShutDown(boolean sd) {
        this.shutDown = sd;
    }

    @Override
    public void run() {

        while (!shutDown) {
            if (lsmCache.size() <= MIN_FLUSH_SIZE) {
                // nothing to flush here
                continue;
            }
            TreeMap<String, KVItem> snapshot = lsmCache.getSnapshot();
            try {
                LSMFile lsmFile = new LSMFile(lsmFileDir);
                for (Map.Entry<String, KVItem> e : snapshot.entrySet()) {
                    lsmFile.append(e.getValue());
                }
                lsmFile.close();
            } catch (IOException e) {
                e.printStackTrace();
                // TODO: Do more than crying?
            }
            try {
                Thread.sleep(CACHE_FLUSH_FREQUENCY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
