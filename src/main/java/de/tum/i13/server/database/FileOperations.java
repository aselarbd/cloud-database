package de.tum.i13.server.database;

import java.io.*;
import java.util.Arrays;
import java.util.logging.Logger;

public class FileOperations {

    private Logger logger = Logger.getLogger(FileOperations.class.getName());

    /**
     * write function writes new key value pairs to database files
     * @param key : key of new tuple
     * @param value : value of new tuple
     * @param fileName : filename that tuple should store
     * @return : returns 1 if process successful
     * @throws IOException : If errors in file writing operation
     */
    synchronized int write(String key, String value, File fileName) {

        try(BufferedWriter bw = new BufferedWriter( new FileWriter(fileName,true) )) {
            bw.write(key+" "+value);
            bw.flush();
            bw.newLine();
        } catch (IOException e) {
            logger.info("error in writing to the database");
            e.printStackTrace();
        }
        return 1;
    }

    /**
     * Update function update and delete a value in the database file
     * @param key : key need to update
     * @param value : Value that need to change
     * @param fileName : File name that update happen
     * @param delete : if operation is delete pass this param as true otherwise false
     * @return : return 1 if update successful
     * @throws IOException : IO exception throws if errors in buffer reader / writer
     */
    synchronized int update(String key, String value, File fileName, Boolean delete)  {

        File tempFile = new File(fileName.getParent() + File.separator + fileName.getName() + "_temp.txt");

        try (BufferedReader br = new BufferedReader( new FileReader(fileName) )  ) {
        try ( BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))) {
            String record;
            while ((record = br.readLine()) != null) {
                String[] keyValuePair = record.split(" ");


                if (keyValuePair[0].equals(key) && !delete) {
                    bw.write(key + " " + value);
                } else if (keyValuePair[0].equals(key) && delete) {
                    continue;
                } else {
                    bw.write(record);
                }
                bw.flush();
                bw.newLine();
            }
            bw.close();
            br.close();

            fileName.delete();
            tempFile.renameTo(fileName);
        }catch (IOException e){
            logger.info("IO exception in update method write buffer");
            e.printStackTrace();
        }
        }catch (IOException e){
            logger.info("IO exception in update method read buffer");
            e.printStackTrace();
        }

        return 1;
    }

    /**
     * getValue function returns value for corresponding key
     * @param key: Key of database tuple
     * @param fileName: File name that database is stored
     * @return : value of given key in database file. If key is absent returns null
     * @throws IOException : If files not found or error with buffer reader or reading the file
     */
    synchronized String getValue(String key, File fileName) {

        try (BufferedReader br = new BufferedReader( new FileReader(fileName) )) {

        String record;
            while ((record = br.readLine()) != null) {
                String[] keyValuePair = record.split(" ");
                if (keyValuePair[0].equals(key)) {
                    br.close();
                    return String.join( " ", Arrays.copyOfRange(keyValuePair, 1, keyValuePair.length) );
                }
            }
        } catch (IOException e) {
            logger.info("IO exception in the getValue method buffer reader");
            e.printStackTrace();
        }
        return null;
    }

}
