package de.tum.i13.server.database;

import java.io.*;
import java.util.Arrays;

/**
 * FileOperations provides basic file ops to store key-value-pairs.
 * No syncronization is done on this level, we assume that all locking has
 * been done on a higher level.
 */
class FileOperations {

    /**
     * write function writes new key value pairs to database files
     * @param key : key of new tuple
     * @param value : value of new tuple
     * @param fileName : filename that tuple should store
     * @throws IOException : If errors in file writing operation
     */
    void write(String key, String value, File fileName) throws IOException {
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(fileName,true))) {
            bw.write(key+" "+value);
            bw.newLine();
        }
    }

    /**
     * Update function update and delete a value in the database file
     * @param key : key need to update
     * @param value : Value that need to change
     * @param fileName : File name that update happen
     * @param delete : if operation is delete pass this param as true otherwise false
     * @throws IOException : IO exception throws if errors in buffer reader / writer
     */
    void update(String key, String value, File fileName, Boolean delete) throws IOException {

        File tempFile = new File(fileName.getParent() + File.separator + fileName.getName() + "_temp.txt");
        try (
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))
        )
        {
            String record;
            while ((record = br.readLine()) != null) {
                String[] keyValuePair = record.split(" ");

                if (keyValuePair[0].equals(key)) {
                    if (!delete) {
                        bw.write(key + " " + value);
                        bw.newLine();
                    }
                } else {
                    bw.write(record);
                    bw.newLine();
                }
            }
        }

        if (!fileName.delete()) {
            throw new IOException("Could not delete outdated file");
        }

        if (!tempFile.renameTo(fileName)) {
            throw new IOException("Could not replace outdated by updated file");
        }
    }

    /**
     * getValue function returns value for corresponding key
     * @param key: Key of database tuple
     * @param fileName: File name that database is stored
     * @throws IOException : If files not found or error with buffer reader or reading the file
     */
    String getValue(String key, File fileName) throws IOException {

        try (BufferedReader br = new BufferedReader( new FileReader(fileName) )) {
            String record;
            while ((record = br.readLine()) != null) {
                String[] keyValuePair = record.split(" ");
                if (keyValuePair[0].equals(key)) {
                    return String.join( " ", Arrays.copyOfRange(keyValuePair, 1, keyValuePair.length) );
                }
            }
        }
        return null;
    }

}
