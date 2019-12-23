package de.tum.i13.server.kv.replication;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.KVTP2ClientFactory;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Replicator {

    private static final Log logger =  new Log(Replicator.class);

    private static final KVItem POISON = new KVItem(Constants.REPLICATION_STOP_MARKER, Constants.REPLICATION_STOP_MARKER);

    private InetSocketAddress address;

    private KVTP2Client ecsClient;
    private final KVStore store;
    private final KVTP2ClientFactory clientFactory;
    private final Map<InetSocketAddress, ReplicationConsumer> kvAddressToReplicationConsumer;
    private final Map<ReplicationConsumer, ExecutorService> services;
    public Replicator(KVStore store) {
        this(null, store);
    }

    public Replicator(
            InetSocketAddress address,
            KVStore store
    ) {
        this(address, store, KVTP2Client::new);
    }

    public Replicator(
            InetSocketAddress address,
            KVStore store,
            KVTP2ClientFactory clientFactory
    ) {
        this.address = address;
        this.store = store;
        this.clientFactory = clientFactory;

        this.kvAddressToReplicationConsumer = new HashMap<>();
        this.services = new HashMap<>();
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public void setEcsClient(KVTP2Client ecsClient) {
        this.ecsClient = ecsClient;
    }

    public void setReplicaSets(ConsistentHashMap keyRange) throws IOException, InterruptedException {
        List<InetSocketAddress> successors = keyRange.getAllSuccessors(address);
        successors.remove(address);

        for (InetSocketAddress successor : successors) {
            if (!kvAddressToReplicationConsumer.keySet().contains(successor)) {
                add(successor);
                replicateAllAt(keyRange, successor);
            }
        }

        Iterator<InetSocketAddress> it = kvAddressToReplicationConsumer.keySet().iterator();
        while (it.hasNext()) {
            InetSocketAddress a = it.next();
            if ((!successors.contains(a))) {
                remove(a);
                it.remove();
            }
        }
    }

    private void remove(InetSocketAddress replica) throws InterruptedException {

        ReplicationConsumer replicationConsumer = kvAddressToReplicationConsumer.get(replica);
        replicationConsumer.add(POISON);

        ExecutorService service = services.get(replicationConsumer);
        service.awaitTermination(5000, TimeUnit.MILLISECONDS);
        service.shutdownNow();
        replicationConsumer.closeClient();
        services.remove(replicationConsumer);
    }

    private void add(InetSocketAddress replica) throws IOException {
        LinkedBlockingQueue<KVItem> replicationQueue = new LinkedBlockingQueue<>();
        ReplicationConsumer replicationConsumer = new ReplicationConsumer(replicationQueue, POISON, getReplicaClient(replica));
        kvAddressToReplicationConsumer.put(replica, replicationConsumer);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(replicationConsumer);
        services.put(replicationConsumer, service);
    }

    /**
     * Initialize a new KVTP2Client to a replica server.
     *
     * @param addr address of the replica server
     * @return a client to talk to the replica server
     */
    private KVTP2Client getReplicaClient(InetSocketAddress addr) throws IOException {
        Message KVToECSMsg = new Message("kv_to_ecs");
        KVToECSMsg.put("kvip", addr.getHostString());
        KVToECSMsg.put("kvport", Integer.toString(addr.getPort()));

        Message response = ecsClient.send(KVToECSMsg);

        KVTP2Client kvtp2Client = clientFactory.get(response.get("ecsip"), Integer.parseInt(response.get("ecsport")));
        kvtp2Client.connect();
        return kvtp2Client;
    }

    public void replicate(KVItem item) throws InterruptedException {
        for (InetSocketAddress inetSocketAddress : kvAddressToReplicationConsumer.keySet()) {
            replicateAt(item, inetSocketAddress);
        }
    }

    private void replicateAt(KVItem item, InetSocketAddress addr) throws InterruptedException {
        kvAddressToReplicationConsumer.get(addr).add(item);
    }

    private void replicateAllAt(ConsistentHashMap keyRange, InetSocketAddress replica) throws IOException {
        Set<String> allKeys = store.getAllKeys((k) -> keyRange.getSuccessor(k).equals(address));
        allKeys.forEach(k -> {
            try {
                replicateAt(store.get(k), replica);
            } catch (InterruptedException e) {
                logger.warning("Interrupted while replicating full keyset", e);
            } catch (IOException e) {
                logger.warning("Could not fetch value for replication of key", e);
            }
        });
    }
}
