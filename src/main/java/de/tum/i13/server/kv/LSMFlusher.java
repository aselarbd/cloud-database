package de.tum.i13.server.kv;

import de.tum.i13.shared.Constants;
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
    private LSMLog lsmLog;

    private boolean shutDown = false;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    public LSMFlusher(LSMCache lsmCache, Path lsmFileDir, LSMLog lsmLog) {

        this.lsmCache = lsmCache;
        this.lsmFileDir = lsmFileDir;
        this.lsmLog = lsmLog;
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
                lsmLog.append(new KVItem(Constants.FLUSH_MESSAGE, "", 0));
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
