package de.tum.i13.kvtp;

import de.tum.i13.shared.Constants;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ServerTest {

    @Test
    public void TestSplitByDelimiter() throws UnsupportedEncodingException {
        byte[] t = "hello\r\nworld".getBytes();

        List<byte[]> bytes = Server.splitByDelimiter(t, "\r\n".getBytes(Constants.TELNET_ENCODING));

        assertArrayEquals("hello".getBytes(), bytes.get(0));
        assertArrayEquals("world".getBytes(), bytes.get(1));
    }

}