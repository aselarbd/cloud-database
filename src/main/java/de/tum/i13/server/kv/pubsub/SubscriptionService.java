package de.tum.i13.server.kv.pubsub;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SubscriptionService {

    private Map<InetSocketAddress, MessageWriter> clientWriters = Collections.synchronizedMap(new HashMap<>());
    private Map<InetSocketAddress, Set<String>> clients = Collections.synchronizedMap(new HashMap<>());
    private Map<String, Set<InetSocketAddress>> subscriptions = Collections.synchronizedMap(new HashMap<>());
    private BlockingQueue<KVItem> changes = new LinkedBlockingQueue<>();

    public void notify(KVItem item) {
        changes.add(item);
    }

    public void subscribe(String key, InetSocketAddress clientAddress, MessageWriter clientWriter) {
        clients.computeIfAbsent(clientAddress, k -> new HashSet<>()).add(key);
        subscriptions.computeIfAbsent(key, k -> new HashSet<>()).add(clientAddress);
        clientWriters.put(clientAddress, clientWriter);
    }

    public void unsubscribe(String key, InetSocketAddress clientAddress) {
        Set<String> subscribedKeys = clients.get(clientAddress);
        Set<InetSocketAddress> clients = subscriptions.get(key);

        if (subscribedKeys != null) {
            subscribedKeys.remove(key);
        }

        if (clients != null) {
            clients.remove(clientAddress);
        }
    }

    public void run() {
        ExecutorService notificationService = Executors.newSingleThreadExecutor();
        notificationService.submit(() -> {
            while (true) {
                KVItem take = changes.take();
                for (InetSocketAddress dest : subscriptions.get(take.getKey())) {
                    notifyClient(dest, take);
                }
            }
        });
    }

    private void notifyClient(InetSocketAddress dest, KVItem update) {
        Message notification = new Message("pubsub_update");
        notification.put("key", update.getKey());

        if (update.getValue().equals(Constants.DELETE_MARKER)) {
            notification.setCommand("delete");
        } else {
            notification.put("value", update.getValue());
        }
        clientWriters.get(dest).write(notification);
    }
}
