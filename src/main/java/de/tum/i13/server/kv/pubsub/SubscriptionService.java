package de.tum.i13.server.kv.pubsub;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SubscriptionService {

    public static final Log logger = new Log(SubscriptionService.class);

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
        if (subscriptions.containsKey(key)) {
            subscriptions.get(key).remove(clientAddress);
        }
        clients.remove(clientAddress);
        clientWriters.remove(clientAddress);
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
    }
}
