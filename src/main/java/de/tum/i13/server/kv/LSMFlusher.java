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
            TreeMap<String, KVItem> snapshot = lsmCache.getSnapshot();
            if (snapshot.size() <= 0) {
                // nothing to flush here
                continue;
            }
            try {
                LSMFile lsmFile = new LSMFile(lsmFileDir);
                for (Map.Entry<String, KVItem> e : snapshot.entrySet()) {
                    if (!lsmFile.append(e.getValue())) {
                        // This should never happen so far
                    }
                    ;
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
