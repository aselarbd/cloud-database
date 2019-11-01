package de.tum.i13.server.database;

import java.io.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;


public class DatabaseManager {

    private String directoryToStore;
    private Logger logger = Logger.getLogger(DatabaseManager.class.getName());

    private File a_d_file ;
    private File e_h_file ;
    private File i_n_file ;
    private File o_s_file ;
    private File t_z_file ;
    private File number_file;
    private File special_file;

    private FileOperations fileOperations;

    private ReadWriteLock rwl = new ReentrantReadWriteLock();

    public DatabaseManager(String directoryToStore) throws IOException {
        this.directoryToStore = directoryToStore;
        init();
        fileOperations = new FileOperations();
        logger.info("DatabaseManager class initialized");
    }

    public String get (String key) throws IOException {
        logger.info("getting a value form DB <key> : "+key);
        File databaseFile = getDatabaseFile(key);


        try {
            rwl.readLock().lock();

            return fileOperations.getValue(key, databaseFile);
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * put a key-value-pair to the database or update it
     *
     * @param key key under which to save the pair
     * @param value value to save
     *
     * @return 0 if a new pair was saved, 1 if a pair was updated under the same key
     *
     * @throws IOException if some file operation fails
     */
    public int put (String key, String value) throws IOException {
        File databaseFile = getDatabaseFile(key);

        try {
            rwl.writeLock().lock();

            if (null == fileOperations.getValue(key, databaseFile)) {
                fileOperations.write(key, value, databaseFile);
                logger.info("write new value to DB successfully");
                return 0;
            } else {
                fileOperations.update(key, value, databaseFile, false);
                logger.info("updated the value in DB for <key> : " + key);
                return 1;
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public void delete (String key) throws IOException {
        File databaseFile = getDatabaseFile(key);

        try {
            rwl.writeLock().lock();

            if (null != fileOperations.getValue(key, databaseFile)) {
                fileOperations.update(key, "", databaseFile, true);
                logger.info("deleted form DB successfully <key> : " + key);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }



    private File getDatabaseFile (String key){
        char control = Character.toLowerCase(key.charAt(0));
        File databaseFile;

        switch (control){
            case 'a': case 'b': case 'c': case 'd':
                databaseFile = a_d_file;
                break;
            case 'e': case 'f': case 'g': case 'h':
                databaseFile = e_h_file;
                break;
            case 'i': case 'j': case 'k': case 'l': case 'm': case 'n':
                databaseFile = i_n_file;
                break;
            case 'o': case 'p': case 'q': case 'r': case 's':
                databaseFile = o_s_file;
                break;
            case 't': case 'u': case 'v': case 'w': case 'x': case 'y': case 'z':
                databaseFile = t_z_file;
                break;
            case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9': case '0':
                databaseFile = number_file;
                break;
            default:
                databaseFile = special_file;
        }
        return databaseFile;
    }


    public String getDirectoryToStore() {
        return directoryToStore;
    }

    public void setDirectoryToStore(String directoryToStore) {
        this.directoryToStore = directoryToStore;
    }

    private void init () throws IOException {

        a_d_file = new File(this.directoryToStore + File.separator + "a_d.txt");
        e_h_file = new File(this.directoryToStore + File.separator + "e_h.txt");
        i_n_file = new File(this.directoryToStore + File.separator + "i_n.txt");
        o_s_file = new File(this.directoryToStore + File.separator + "o_s.txt");
        t_z_file = new File(this.directoryToStore + File.separator + "t_z.txt");
        number_file = new File(this.directoryToStore + File.separator + "number.txt");
        special_file = new File(this.directoryToStore + File.separator + "special.txt");

        if (!a_d_file.createNewFile())
            logger.info("Error in creating a_d_file");
        if(!e_h_file.createNewFile())
            logger.info("Error in creating e_h_file");
        if(!i_n_file.createNewFile())
            logger.info("Error in creating i_n_file");
        if(!o_s_file.createNewFile())
            logger.info("Error in creating o_s_file");
        if(!t_z_file.createNewFile())
            logger.info("Error in creating t_z_file");
        if(!number_file.createNewFile())
            logger.info("Error in creating number_file");
        if(!special_file.createNewFile())
            logger.info("Error in creating special_file");
    }

}
