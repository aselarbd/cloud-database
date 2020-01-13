package de.tum.i13.server.kv.pubsub;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;
import de.tum.i13.shared.TaskRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SubscriptionService {

    private static final Log logger = new Log(SubscriptionService.class);

    private TaskRunner taskRunner;

    private Map<InetSocketAddress, KVTP2Client> replicaClients = new HashMap<>();
    private Map<String, List<KVItem>> replicatedNotifications = Collections.synchronizedMap(new HashMap<>());

    private Map<InetSocketAddress, MessageWriter> clientWriters = Collections.synchronizedMap(new HashMap<>());
    private Map<InetSocketAddress, Set<String>> clients = Collections.synchronizedMap(new HashMap<>());
    private Map<String, Set<InetSocketAddress>> subscriptions = Collections.synchronizedMap(new HashMap<>());
    private BlockingQueue<KVItem> changes = new LinkedBlockingQueue<>();

    private Replicator replicator;

    public SubscriptionService(Replicator replicator, TaskRunner taskRunner) {
        this.replicator = replicator;
        this.taskRunner = taskRunner;
    }

    public void notify(KVItem item) {
        changes.add(item);
        run();
    }

    public void replicateNotification(KVItem item) {
        replicatedNotifications.computeIfAbsent(item.getKey(), k -> new LinkedList<>()).add(item);
    }

    public void cancelNotification(KVItem item) {
        if (replicatedNotifications.containsKey(item.getKey())) {
            replicatedNotifications.get(item.getKey()).remove(item);
        }
    }

    public void takeResponsibility(ConsistentHashMap keyRange, InetSocketAddress address) {
        Iterator<Map.Entry<String, List<KVItem>>> iterator = replicatedNotifications.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<KVItem>> next = iterator.next();
            if (keyRange.getSuccessor(next.getKey()).equals(address)) {
                changes.addAll(next.getValue());
                iterator.remove();
            }
        }
        run();
    }

    public void subscribe(String key, InetSocketAddress clientAddress, MessageWriter clientWriter) {
        clients.computeIfAbsent(clientAddress, k -> new HashSet<>()).add(key);
        subscriptions.computeIfAbsent(key, k -> new HashSet<>()).add(clientAddress);
        clientWriters.put(clientAddress, clientWriter);
    }

    public void unsubscribe(String key, InetSocketAddress clientAddress) {
        if (subscriptions.containsKey(key)) {
            subscriptions.get(key).remove(clientAddress);
        }
        clients.remove(clientAddress);
        clientWriters.remove(clientAddress);
    }

    public void run() {
        taskRunner.run(() -> {
            while (changes.size() > 0) {
                try {
                    KVItem take = changes.take();
                    for (InetSocketAddress dest : subscriptions.get(take.getKey())) {
                        notifyClient(dest, take);
                    }
                } catch (InterruptedException e) {
                    // all fine
                } catch (IOException e) {
                    logger.warning("notifyClient failed", e);
                }
            }
        });
    }

    private void notifyClient(InetSocketAddress dest, KVItem update) throws IOException {
        Message notification = new Message("pubsub_update");
        notification.put("key", update.getKey());

        if (update.getValue().equals(Constants.DELETE_MARKER)) {
            notification.setCommand("delete");
        } else {
            notification.put("value", update.getValue());
        }

        try {
            clientWriters.get(dest).write(notification);
        } catch (CancelledKeyException e) {
            logger.info("Client has gone away, cancelling subscription");
            clientWriters.get(dest).close();
            unsubscribe(update.getKey(), dest);
        }

        Set<InetSocketAddress> currentReplicaSet = replicator.getCurrentReplicaSet();

        for (InetSocketAddress inetSocketAddress : currentReplicaSet) {
            if (!replicaClients.containsKey(inetSocketAddress)) {
                connectReplica(inetSocketAddress);
            }
        }
        Iterator<Map.Entry<InetSocketAddress, KVTP2Client>> iterator = replicaClients.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<InetSocketAddress, KVTP2Client> next = iterator.next();
            if (currentReplicaSet.contains(next.getKey())) {
                next.getValue().close();
                iterator.remove();
            }
        }

        for (KVTP2Client client : replicaClients.values()) {
            Message message = new Message("cancelnotification");
            message.put("key", update.getKey());
            message.put("value", update.getValue());
            try {
                client.send(message);
            } catch (IOException e) {
                logger.warning("Replication lost: ", e);
            }
        }
    }

    private void connectReplica(InetSocketAddress inetSocketAddress) throws IOException {
        replicaClients.put(inetSocketAddress, replicator.getReplicaClient(inetSocketAddress));
    }
}
