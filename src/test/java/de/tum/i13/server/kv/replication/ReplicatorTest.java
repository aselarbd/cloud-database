package de.tum.i13.server.kv.replication;

import de.tum.i13.TestConstants;
import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.KVTP2ClientFactory;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.TaskRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ReplicatorTest {
    Replicator replicator;
    ConsistentHashMap testMap;
    KVStore kvStore;
    KVTP2Client ecsClient;
    KVTP2Client kvClient;
    KVTP2ClientFactory clientFactory;

    private static int REPL_THREAD_WAIT = 50;
    private static int SHUTDOWN_WAIT = 5050;

    @BeforeEach
    public void prepareTest() throws IOException {
        ecsClient = mock(KVTP2Client.class);
        Message ecsResponse = new Message("kv_to_ecs");
        ecsResponse.put("ecsip", "127.0.0.1");
        ecsResponse.put("ecsport", "99");
        when(ecsClient.send(any(Message.class))).thenReturn(ecsResponse);
        kvClient = mock(KVTP2Client.class);
        clientFactory = mock(KVTP2ClientFactory.class);
        when(clientFactory.get(anyString(), anyInt())).thenReturn(kvClient);
        kvStore = mock(KVStore.class);
        testMap = ConsistentHashMap.fromKeyrangeReadString(TestConstants.KEYRANGE_REPLICA_FULL);
    }

    private void prepareRepl(InetSocketAddress ip) {
        replicator = new Replicator(ip, kvStore, clientFactory);
        replicator.setEcsClient(ecsClient);
    }

    private void assertECSQuery(String allIps) throws IOException {
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(ecsClient, times(2)).send(captor.capture());

        for (Message m : captor.getAllValues()) {
            allIps = allIps.replace(m.get("kvip"), "");
        }
        allIps = allIps.replaceAll(",", "");
        assertEquals("", allIps);
    }

    private void assertClientSend(String cmd, String key, String val) throws IOException {
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        verify(kvClient, times(2)).send(captor.capture());
        for (Message m : captor.getAllValues()) {
            assertEquals(cmd, m.getCommand());
            assertEquals(key, m.get("key"));
            if (val != null) {
                assertEquals(val, m.get("value"));
            }
        }
    }

    @Test
    public void testReplicatePutDelete() throws IOException, InterruptedException {
        prepareRepl(TestConstants.IP_1);

        replicator.setReplicaSets(testMap);

        // check if ECS was queried for the correct IPs (IP 2 and IP 3)
        assertECSQuery(TestConstants.IP_2.getHostName() + "," + TestConstants.IP_3.getHostName());

        replicator.replicate(new KVItem("testkey", "value test"));

        Thread.sleep(REPL_THREAD_WAIT);

        assertClientSend("put", "testkey", "value test");
        reset(kvClient);

        // update
        replicator.replicate(new KVItem("testkey", "1234ab"));

        Thread.sleep(REPL_THREAD_WAIT);

        assertClientSend("put", "testkey", "1234ab");
        reset(kvClient);

        // delete
        replicator.replicate(new KVItem("testkey"));

        Thread.sleep(REPL_THREAD_WAIT);

        assertClientSend("delete", "testkey", null);
    }

    @Test
    public void testReplicateDifferentIp() throws IOException, InterruptedException {
        prepareRepl(TestConstants.IP_2);

        replicator.setReplicaSets(testMap);

        assertECSQuery(TestConstants.IP_1.getHostName() + "," + TestConstants.IP_3.getHostName());
    }


    @Test
    public void testReplicateDisconnect() throws IOException, InterruptedException {
        prepareRepl(TestConstants.IP_1);
        replicator.setReplicaSets(testMap);

        Thread.sleep(REPL_THREAD_WAIT);

        // two instances are not replicated. Ensure both replica get disconnected.
        testMap.remove(TestConstants.IP_2);
        testMap = testMap.getInstanceWithReplica();

        Thread.sleep(REPL_THREAD_WAIT);

        replicator.setReplicaSets(testMap);

        Thread.sleep(SHUTDOWN_WAIT);

        verify(kvClient, times(2)).close();

        replicator.replicate(new KVItem("foo", "bar"));

        verify(kvClient, never()).send(any(Message.class));
    }
}
