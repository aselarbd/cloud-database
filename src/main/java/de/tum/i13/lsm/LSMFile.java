package de.tum.i13.lsm;

import de.tum.i13.shared.KVItem;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeMap;

/**
 * LSMFile provides an interface to LSMFiles on disk. An LSMFile is actually
 * a directory containing an index and a data file. The data file contains KVItems
 * while the index file only contains keys and for each key the position in the data
 * file where the item to the key is stored.
 */
public class LSMFile implements Closeable {

    private static final String DATA_FILE_NAME = "data-";
    private static final String INDEX_FILE_NAME = "index-";
    protected static final int KEY_LENGTH = 20; // Bytes

    private File data;
    private File index;

    private FileOutputStream dataFOS;
    private FileOutputStream indexFOS;

    private FileInputStream dataFIS;
    private FileInputStream indexFIS;

    private String currentKey;

    private boolean closed;

    /**
     * default constructor for subclasses
     */
    protected LSMFile() {

    }

    /**
     * Constructor for reading an existing LSMFile
     * @param directory location of the LSMFile (which is a directory containing data and index file)
     * @param name name of the LSMFile
     * @throws FileNotFoundException If the file can't be found or
     */
    public LSMFile(Path directory, String name) throws FileNotFoundException {
        closed = true;
        Path fp = Paths.get(directory.toString(), name);
        if (!Files.exists(fp)) {
            throw new FileNotFoundException();
        }

        String dataName = Paths.get(fp.toString(), DATA_FILE_NAME + name).toString();
        this.data = new File(dataName);

        String indexName = Paths.get(fp.toString(), INDEX_FILE_NAME + name).toString();
        this.index = new File(indexName);

        openRead();
    }

    /**
     * Constructor to create a new LSMFile
     *
     * @param directory location where the LSMFile will be saved
     */
    public LSMFile(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }

        String name = getRandomName();
        Path dataPath = Paths.get(directory.toString(), name, DATA_FILE_NAME + name);

        while(Files.exists(dataPath)) {
            name = getRandomName();
            dataPath = Paths.get(directory.toString(), name, DATA_FILE_NAME + name);
        }

        this.data = new File(dataPath.toString());

        String indexName = Paths.get(directory.toString(), name, INDEX_FILE_NAME + name).toString();
        this.index = new File(indexName);

        openWrite();
    }

    /**
     * open the LSMFile for write access
     *
     * @throws IOException if some IO operation fails
     */
    private void openWrite() throws IOException {
        if (closed) {
            throw new IOException("Write to closed or immutable file");
        }

        Files.createDirectories(Paths.get(data.getParent()));

        Files.createFile(Paths.get(data.getPath()));
        Files.createFile(Paths.get(index.getPath()));

        dataFOS = new FileOutputStream(data);
        indexFOS = new FileOutputStream(index);
    }

    /**
     * open the LSMFile for read access
     *
     * @throws FileNotFoundException if the file does not exist
     */
    private void openRead() throws FileNotFoundException {
        dataFIS = new FileInputStream(data);
        indexFIS = new FileInputStream(index);
    }

    /**
     * creates a new random name for a new LSMFile
     *
     * @return a random string
     */
    private String getRandomName() {
        Random rand = new Random();
        return "" + Math.abs(rand.nextLong());
    }

    /**
     * append a new KVItem to the LSMFile. This method writes the KVItem to the
     * data file and the key and the position in the data file to the index file.
     *
     * The data file has the following format for each item:
     *
     * 20 bytes "paddedKeyBytes" key
     * 8 bytes "keyLengthBytes" indicating the actual key length (may be shorter than 20 bytes)
     * 8 bytes "timestampBytes" timestamp
     * 8 bytes "valueLengthBytes" indicating the length of the value
     * valueLengthBytes bytes actual value
     *
     * The index file has the following format for each item:
     *
     * 20 bytes "paddedKeyBytes" key
     * 8 bytes "keyLengthBytes" indicating the actual key length (may be shorter than 20 bytes)
     * 8 bytes "positionBytes" pointing to the position of this item in the data file
     *
     * @param item item to append
     *
     * @return true if the item was appended, false if the item is to late
     * (i.e. another item which is later in lexicographical order than this item
     * has already been saved to the file in the past. In this case, appending
     * this item would destroy the lexicographic sort order).
     *
     * @throws IOException If some IOError occurs
     */
    public boolean append(KVItem item) throws IOException {
        if (currentKey != null && currentKey.compareTo(item.getKey()) > 0) {
            return false;
        }

        long position = data.length();

        byte[] paddedKeyBytes = padKey(item.getKey());
        byte[] keyLengthBytes = longToBytes(item.getKey().getBytes().length);
        byte[] positionBytes = longToBytes(position);

        byte[] timestampBytes = longToBytes(item.getTimestamp());
        byte[] valueBytes = item.getValue().getBytes();
        byte[] valueLengthBytes = longToBytes(valueBytes.length);

        indexFOS.write(paddedKeyBytes);
        indexFOS.write(keyLengthBytes);
        indexFOS.write(positionBytes);

        dataFOS.write(paddedKeyBytes);
        dataFOS.write(keyLengthBytes);
        dataFOS.write(timestampBytes);

        dataFOS.write(valueLengthBytes);
        dataFOS.write(valueBytes);

        indexFOS.flush();
        dataFOS.flush();

        currentKey = item.getKey();
        return true;
    }

    /**
     * reads the index of an LSMFile and returns it as a TreeMap
     * from key to position in the data file
     *
     * @return TreeMap containing the index or null if the file is a write only file
     * @throws IOException when some IO Error occurs on reading the index file
     */
    public TreeMap<String, Long> readIndex() throws IOException {
        if (indexFIS == null) {
            // reading on write only file
            return null;
        }

        TreeMap<String, Long> index = new TreeMap<>();

        while(indexFIS.available() > 0) {
            byte[] key = new byte[KEY_LENGTH];
            indexFIS.read(key, 0, KEY_LENGTH);

            byte[] keyLengthBytes = new byte[Long.BYTES];
            indexFIS.read(keyLengthBytes, 0, Long.BYTES);
            int keyLength = (int) bytesToLong(keyLengthBytes);

            byte[] position = new byte[Long.BYTES];
            indexFIS.read(position, 0, Long.BYTES);
            index.put(new String(Arrays.copyOfRange(key, 0, keyLength)), bytesToLong(position));
        }

        return index;
    }

    /**
     * Read a value from the data file of an LSMFile
     *
     * @param position at which the value begins
     *
     * @return the KVItem at the position in the data file
     *
     * @throws IOException If some IO Error occurs while reading the data file
     */
    public KVItem readValue(long position) throws IOException {
        long skip = dataFIS.skip(position); // TODO: Maybe handle the result?

        byte[] key = new byte[KEY_LENGTH];
        dataFIS.read(key, 0, KEY_LENGTH);

        byte[] keyLengthBytes = new byte[Long.BYTES];
        dataFIS.read(keyLengthBytes, 0, Long.BYTES);
        int keyLength = (int) bytesToLong(keyLengthBytes);

        byte[] timestampBytes = new byte[Long.BYTES];
        dataFIS.read(timestampBytes, 0, Long.BYTES);
        long timestamp = bytesToLong(timestampBytes);

        byte[] lengthBytes = new byte[Long.BYTES];
        dataFIS.read(lengthBytes, 0, Long.BYTES);

        int length = (int) bytesToLong(lengthBytes);
        byte[] valueBytes = new byte[length];
        dataFIS.read(valueBytes, 0, length);

        return new KVItem(new String(Arrays.copyOfRange(key, 0, keyLength)), new String(valueBytes), timestamp);
    }

    /**
     * get a long value as byte array
     *
     * @param l long value
     * @return a byte array containing the long value as bytes
     */
    protected byte[] longToBytes(long l) {
        ByteBuffer res = ByteBuffer.allocate(Long.BYTES);
        ByteBuffer byteBuffer = res.putLong(l);
        return byteBuffer.array();
    }

    /**
     * Get a long value from a byte array
     *
     * @param bytes the byte array holding the long value
     *
     * @return the long value which was stored in the byte array
     */
    protected long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    /**
     * Write a key to a padded byte array of size LSMFile.KEY_LENGTH
     *
     * @param key key to store in a byte array
     *
     * @return a byte array holding the padded key as bytes
     */
    protected byte[] padKey(String key) {
        byte[] res = new byte[LSMFile.KEY_LENGTH];
        byte[] keyBytes = key.getBytes();

        System.arraycopy(keyBytes, 0, res, 0, keyBytes.length);
        return res;
    }

    /**
     * close this LSMFile
     *
     * @throws IOException if the file can't be closed due to some IO Error
     */
    @Override
    public void close() throws IOException {
        if (dataFOS != null) {
            dataFOS.close();
        }
        if (indexFOS != null) {
            indexFOS.close();
        }
        if (dataFIS != null) {
            dataFIS.close();
        }
        if (indexFIS != null) {
            indexFIS.close();
        }
        closed = true;
    }

    /**
     * returns the name of this LSMFile
     *
     * @return name of the LSMFile
     */
    public String getName() {
        return Paths.get(data.getParent()).getFileName().toString();
    }
}
