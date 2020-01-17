package de.tum.i13.server.kv.pubsub;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.shared.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SubscriptionService {

    private static final Log logger = new Log(SubscriptionService.class);

    private TaskRunner taskRunner;

    private final Map<InetSocketAddress, KVTP2Client> replicaClients = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, List<KVItem>> replicatedNotifications = Collections.synchronizedMap(new HashMap<>());

    private final Map<InetSocketAddress, MessageWriter> clientWriters = Collections.synchronizedMap(new HashMap<>());
    private final Map<InetSocketAddress, Set<String>> clients = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Set<InetSocketAddress>> subscriptions = Collections.synchronizedMap(new HashMap<>());
    private final BlockingQueue<KVItem> changes = new LinkedBlockingQueue<>();

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
        synchronized (replicatedNotifications) {
            if (replicatedNotifications.containsKey(item.getKey())) {
                replicatedNotifications.get(item.getKey()).remove(item);
            }
        }
    }

    public void takeResponsibility(ConsistentHashMap keyRange, InetSocketAddress address) {
        publishKeyRange(keyRange);
        synchronized (replicatedNotifications) {
            Iterator<Map.Entry<String, List<KVItem>>> iterator = replicatedNotifications.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<KVItem>> next = iterator.next();
                if (keyRange.getSuccessor(next.getKey()).equals(address)) {
                    changes.addAll(next.getValue());
                    iterator.remove();
                }
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
        synchronized (subscriptions) {
            if (subscriptions.containsKey(key)) {
                subscriptions.get(key).remove(clientAddress);
            }
        }
        clients.remove(clientAddress);
        clientWriters.remove(clientAddress);
    }

    public void run() {
        taskRunner.run(() -> {
            synchronized (changes) {
                while (changes.size() > 0) {
                    logger.info(changes.stream().map(kvItem -> kvItem.toString()).reduce((s, s2) -> s + s2).orElse(""));
                    try {
                        KVItem take = changes.take();
                        Set<InetSocketAddress> destinations = subscriptions.getOrDefault(take.getKey(),
                                new HashSet<>());
                        for (InetSocketAddress dest : destinations) {
                            logger.info(dest.toString());
                            notifyClient(dest, take);
                        }
                    } catch (InterruptedException e) {
                        // all fine
                    } catch (Exception e) {
                        // log all exceptions to be aware of thread crashes
                        logger.warning("notifyClient failed", e);
                    }
                }
            }
        });
    }

    private void sendMsg(InetSocketAddress dest, Message msg) {
        try {
            clientWriters.get(dest).write(msg);
            clientWriters.get(dest).flush();
        } catch (CancelledKeyException e) {
            logger.info("Client has gone away, cancelling subscription");
            try {
                clientWriters.get(dest).close();
                unsubscribe(msg.get("key"), dest);
            } catch (IOException ex) {
                logger.info("failed to close connection to lost client", ex);
            }
        }

    }

    private void publishKeyRange(ConsistentHashMap keyRange) {
        taskRunner.run(() -> {
            for (InetSocketAddress inetSocketAddress : clients.keySet()) {
                notifyClientKeyRange(inetSocketAddress, keyRange);
            }
        });
    }

    private void notifyClientKeyRange(InetSocketAddress dest, ConsistentHashMap keyRange) {
        Message msg = new Message("keyrange_update");
        msg.put("keyrange", keyRange.getKeyrangeReadString());
        sendMsg(dest, msg);
    }

    private void notifyClient(InetSocketAddress dest, KVItem update) throws IOException {
        Message notification = new Message("pubsub_update");
        notification.put("key", update.getKey());

        // for deletes, just omit the value field
        if (!update.getValue().equals(Constants.DELETE_MARKER)) {
            notification.put("value", update.getValue());
        }

        sendMsg(dest, notification);
        updateReplica(update);
    }

    private void updateReplica(KVItem update) throws IOException {
        Set<InetSocketAddress> currentReplicaSet = replicator.getCurrentReplicaSet();

        for (InetSocketAddress inetSocketAddress : currentReplicaSet) {
            if (!replicaClients.containsKey(inetSocketAddress)) {
                connectReplica(inetSocketAddress);
            }
        }

        synchronized (replicaClients) {
            Iterator<Map.Entry<InetSocketAddress, KVTP2Client>> iterator = replicaClients.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<InetSocketAddress, KVTP2Client> next = iterator.next();
                if (currentReplicaSet.contains(next.getKey())) {
                    next.getValue().close();
                    iterator.remove();
                }
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
