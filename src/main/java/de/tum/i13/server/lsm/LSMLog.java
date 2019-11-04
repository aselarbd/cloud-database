package de.tum.i13.server.lsm;

import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSMLog provides access to an append log file for KVItem operations
 */
public class LSMLog extends LSMFile {

    private int size;

    private Path logFile;
    private FileOutputStream out;
    private FileInputStream in;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    /**
     * open a new LSMLog-file
     *
     * @param dir where to find or create the log file
     *
     * @throws IOException if an error occurs when accessing the file
     */
    public LSMLog(Path dir) throws IOException {
        Path file = dir;
        if(!dir.toFile().exists()) {
            file = Files.createFile(dir).toAbsolutePath();
        }
        this.logFile = file;

        this.out = new FileOutputStream(logFile.toFile(), true);
        this.in = new FileInputStream(logFile.toFile());
    }

    /**
     * append a KVItem to the end of the log file
     *
     * @param kvItem KVItem to append
     *
     * @return true
     * @throws IOException throws an exception if there's some IO Error
     */
    @Override
    public boolean append(KVItem kvItem) throws IOException {

        rwl.writeLock().lock();
        try {
            byte[] paddedKeyBytes = padKey(kvItem.getKey());
            byte[] keyLengthBytes = longToBytes(kvItem.getKey().getBytes().length);
            byte[] timestampBytes = longToBytes(kvItem.getTimestamp());
            byte[] valueBytes = kvItem.getValue().getBytes();
            byte[] valueLengthBytes = longToBytes(valueBytes.length);

            out.write(paddedKeyBytes);
            out.write(keyLengthBytes);
            out.write(timestampBytes);

            out.write(valueLengthBytes);
            out.write(valueBytes);

            out.flush();
            size++;
        } finally {
            rwl.writeLock().unlock();
        }

        return true;
    }

    /**
     * read all entries from the log file since the last Constants.FLUSH_MESSAGE
     * has been written to the log
     *
     * @return a TreeMap containing all the items since the last flush message
     *
     * @throws IOException if an IO Error occurs while accessing the log file
     */
    public TreeMap<String, KVItem> readAllSinceFlush() throws IOException {

        rwl.readLock().lock();
        try {
            TreeMap<String, KVItem> map = new TreeMap<>();

            while (in.available() > 0) {
                byte[] keyBytes = new byte[KEY_LENGTH];
                in.read(keyBytes, 0, KEY_LENGTH);

                byte[] keyLengthBytes = new byte[Long.BYTES];
                in.read(keyLengthBytes, 0, Long.BYTES);
                int keyLength = (int) bytesToLong(keyLengthBytes);

                byte[] timestampBytes = new byte[Long.BYTES];
                in.read(timestampBytes, 0, Long.BYTES);
                long timestamp = bytesToLong(timestampBytes);

                byte[] lengthBytes = new byte[Long.BYTES];
                in.read(lengthBytes, 0, Long.BYTES);

                int length = (int) bytesToLong(lengthBytes);
                byte[] valueBytes = new byte[length];
                in.read(valueBytes, 0, length);

                String key = new String(Arrays.copyOfRange(keyBytes, 0, keyLength));

                if (key.equals(Constants.FLUSH_MESSAGE)) {
                    // Everything read so far had already been flushed and can thus be discarded
                    map = new TreeMap<>();
                } else {
                    KVItem kvItem = new KVItem(new String(Arrays.copyOfRange(keyBytes, 0, keyLength)), new String(valueBytes), timestamp);
                    map.put(kvItem.getKey(), kvItem);
                }
            }

            return map;
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * returns the current count of items written to the log file
     *
     * @return count of items in the log
     */
    public int size() {
        return size;
    }

}
