package de.tum.i13.shared.parsers;

import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.ECSMessage;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

public class ECSMessageParserTest {
    ECSMessageParser parser = new ECSMessageParser();

    @Test
    public void parseUnknownMessage() {
        assertNull(parser.parse("0123"));
    }

    @Test
    public void parseInvalidArgs() {
        // wrong type
        assertNull(parser.parse("register foo"));
        // wrong argument count
        assertNull(parser.parse("register 127.0.0.1:80 bar"));
    }

    @Test
    public void parseNoArg() {
        ECSMessage msg = parser.parse("ok");
        assertNotNull(msg);
        assertEquals(ECSMessage.MsgType.RESPONSE_OK, msg.getType());
    }

    @Test
    public void parseOneArg() {
        ECSMessage msg = parser.parse("register 127.0.0.1:82");
        assertNotNull(msg);
        assertEquals(ECSMessage.MsgType.REGISTER_SERVER, msg.getType());
        assertEquals(new InetSocketAddress("127.0.0.1", 82), msg.getIpPort(0));
    }

    @Test
    public void parseMultipleArgs() {
        ECSMessage msg = parser.parse("transfer_range 127.0.0.1:82 10.1.2.3:5678 127.0.0.1:85");
        assertNotNull(msg);
        assertEquals(ECSMessage.MsgType.TRANSFER_RANGE, msg.getType());
        assertEquals(new InetSocketAddress("127.0.0.1", 82), msg.getIpPort(0));
        assertEquals(new InetSocketAddress("10.1.2.3", 5678), msg.getIpPort(1));
        assertEquals(new InetSocketAddress("127.0.0.1", 85), msg.getIpPort(2));
    }

    @Test
    public void parseBase64() {
        ECSMessage msg = parser.parse("ecs_put Zm9vYmFy");
        assertNotNull(msg);
        assertEquals(ECSMessage.MsgType.ECS_PUT, msg.getType());
        assertEquals("foobar", msg.getBase64(0));
    }

    @Test
    public void parseKeyrange() throws NoSuchAlgorithmException {
        ECSMessage msg = parser.parse("keyrange be8e4f546de43337d7f0d4637a796478," +
                "be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;");
        assertNotNull(msg);
        assertEquals(ECSMessage.MsgType.KEYRANGE, msg.getType());
        ConsistentHashMap testMap = msg.getKeyrange(0);
        assertEquals(new InetSocketAddress("192.168.1.1", 80), testMap.get("test"));
    }
}
