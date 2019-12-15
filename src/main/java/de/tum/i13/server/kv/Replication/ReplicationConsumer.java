package de.tum.i13.server.kv.Replication;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class ReplicationConsumer implements Runnable {

    private static final Logger logger = Logger.getLogger(ReplicationConsumer.class.getName());

    private final BlockingQueue<KVItem> replicationQueue;
    private final KVItem poison;
    private final KVTP2Client client;

    public ReplicationConsumer(BlockingQueue<KVItem> replicationQueue, KVItem poison, KVTP2Client client) {
        this.replicationQueue = replicationQueue;
        this.poison = poison;
        this.client = client;
    }

    public void add(KVItem item) throws InterruptedException {
        replicationQueue.put(item);
    }

    @Override
    public void run() {
        KVItem next;
        while (true) {
            Message message = new Message("put");
            try {
                if (!(next = replicationQueue.take()).equals(poison)) break;
                message.put("key", next.getKey());
                message.put("value", next.getValue());
                message.put("timestamp", Long.toString(next.getTimestamp()));
                client.send(message);
            } catch (InterruptedException e) {
                logger.warning("interrupted while waiting for more replica items");
            } catch (IOException e) {
                logger.warning("failed to send item to replica: " + e.getMessage());
            }
        }
    }
}
