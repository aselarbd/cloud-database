package de.tum.i13.server.kv.state;

import de.tum.i13.server.kv.state.requests.Request;
import de.tum.i13.server.kv.state.requests.RequestType;
import de.tum.i13.shared.ConsistentHashMap;
import org.junit.jupiter.api.Test;

class ServerTest {

    @Test
    public void testChangeState() {
        Server server = new Server(new ConsistentHashMap());
        server.changeState(new Request(RequestType.LOCK));
        server.changeState(new Request(RequestType.SHUTDOWN));
    }

}