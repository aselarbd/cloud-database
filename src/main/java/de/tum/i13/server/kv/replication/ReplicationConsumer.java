package de.tum.i13.server.kv.replication;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class ReplicationConsumer implements Runnable {

    private static final Log logger = new Log(ReplicationConsumer.class);

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

    public void closeClient() {
        try {
            this.client.close();
        } catch (IOException e) {
            logger.warning("Failed to close consumer client", e);
        }
    }

    @Override
    public void run() {
        KVItem next;
        while (true) {
            Message message = new Message("put");
            try {
                if ((next = replicationQueue.take()).equals(poison)) break;
                message.put("key", next.getKey());
                message.put("value", next.getValue());
                if (next.getValue() == null) {
                    message.setCommand("delete");
                }
                message.put("timestamp", Long.toString(next.getTimestamp()));
                client.send(message);
            } catch (InterruptedException e) {
                logger.warning("Interrupted while waiting for more replica items");
            } catch (IOException e) {
                logger.warning("Failed to send item to replica", e);
            }
        }
    }
}
