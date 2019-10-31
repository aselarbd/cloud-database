package de.tum.i13.server.database;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FileOperationsTest {

    private FileOperations fileOperationsTest = new FileOperations();

    @Test
    void write() throws IOException {
        File writeTest = new File(System.getProperty("user.dir")+"\\writeTest.txt");
        if( writeTest.createNewFile()){
            fileOperationsTest.write("jbl","music head",writeTest);
            assertEquals("music head",fileOperationsTest.getValue("jbl",writeTest));
        }
        Boolean success = writeTest.delete();
    }

    @Test
    void update_one() throws IOException {
        File updateTest = new File(System.getProperty("user.dir")+"\\updateTest.txt");
        if( updateTest.createNewFile()){
            fileOperationsTest.write("jbl","music",updateTest);
            fileOperationsTest.update("jbl","speaker",updateTest,false);
            assertEquals("speaker",fileOperationsTest.getValue("jbl",updateTest));

            Boolean success = updateTest.delete();
        }

    }

    @Test
    void update_two() throws IOException {
        File updateTest = new File(System.getProperty("user.dir")+"\\updateTest.txt");
        if( updateTest.createNewFile()){
            fileOperationsTest.write("jbl","music",updateTest);
            fileOperationsTest.update("jbl","speaker",updateTest,false);
            assertNotEquals("music",fileOperationsTest.getValue("jbl",updateTest));

            Boolean success = updateTest.delete();
        }

    }

    @Test
    void update_three() throws IOException {
        File updateTest = new File(System.getProperty("user.dir")+"\\updateTest.txt");
        if( updateTest.createNewFile()){
            fileOperationsTest.write("jbl","music",updateTest);
            assertEquals(1,fileOperationsTest.update("jbl","speaker",updateTest,true));
            assertNull(fileOperationsTest.getValue("jbl", updateTest));

            Boolean success = updateTest.delete();
        }

    }

    @Test
    void getValue() throws IOException {
        File getTest = new File(System.getProperty("user.dir")+"\\getTest.txt");
        if( getTest.createNewFile()){
            fileOperationsTest.write("jbl","music",getTest);
            fileOperationsTest.write("hp","laptop",getTest);
            fileOperationsTest.update("jbl","speaker",getTest,false);
            assertEquals("speaker",fileOperationsTest.getValue("jbl",getTest));
            assertEquals("laptop",fileOperationsTest.getValue("hp",getTest));
            assertNull(fileOperationsTest.getValue("notExist", getTest));

            Boolean success = getTest.delete();
        }
    }
}