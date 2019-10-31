package de.tum.i13.server.database;

import java.io.*;
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

    public DatabaseManager(String directoryToStore){
        this.directoryToStore = directoryToStore;
        try {
            init();
            fileOperations = new FileOperations();
            logger.info("DatabaseManager class initialized");
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("Error in the creating file operation class");
        }
    }

    public String get (String key){
        File databaseFile = getDatabaseFile(key);
        String value = null;
        value = fileOperations.getValue(key,databaseFile);
        logger.info("getting a value form DB <key> : "+key);
        return value;
    }

    public int put (String key, String value){
        File databaseFile = getDatabaseFile(key);

        if (null == fileOperations.getValue(key,databaseFile)){
            if (1 == fileOperations.write(key,value,databaseFile))
                logger.info("write new value to DB successfully");
                return 1;

        }else {
            if(1 == fileOperations.update(key,value,databaseFile,false)){
                logger.info("updated the value in DB for <key> : "+key);
                return 2;
            }
        }
        logger.info("Error in putting value to the database");
        return -1;
    }

    public int delete (String key){
        File databaseFile = getDatabaseFile(key);

        if (null != fileOperations.getValue(key,databaseFile)){
            if(1 == fileOperations.update(key,"",databaseFile,true))
                logger.info("deleted form DB successfully <key> : "+key);
                return 1;
        }
        logger.info("Error in the deletion path");
        return -1;
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

        a_d_file = new File(this.directoryToStore+"\\a_d.txt");
        e_h_file = new File(this.directoryToStore+"\\e_h.txt");
        i_n_file = new File(this.directoryToStore+"\\i_n.txt");
        o_s_file = new File(this.directoryToStore+"\\o_s.txt");
        t_z_file = new File(this.directoryToStore+"\\t_z.txt");
        number_file = new File(this.directoryToStore+"\\number.txt");
        special_file = new File(this.directoryToStore+"\\special.txt");

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
