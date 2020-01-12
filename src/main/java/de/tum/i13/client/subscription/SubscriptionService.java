package de.tum.i13.client.subscription;

import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SubscriptionService {
    private static final Log logger = new Log(SubscriptionService.class);
    Map<InetSocketAddress, Subscriber> subscribers = new HashMap<>();
    Set<String> subscribedKeys = new HashSet<>();
    ConsistentHashMap keyrange = null;
    Supplier<ConsistentHashMap> keyrangeUpdater;
    Consumer<KVItem> updateHandler;
    Consumer<String> outputHandler;

    public SubscriptionService(Supplier<ConsistentHashMap> keyrangeUpdater, Consumer<KVItem> updateHandler,
                               Consumer<String> outputHandler) {
        this.keyrangeUpdater = keyrangeUpdater;
        this.updateHandler = updateHandler;
        this.outputHandler = outputHandler;
    }

    private String subscribeOrUnsubscribe(String key, Consumer<Subscriber> action) {
        if (keyrange == null) {
            keyrange = keyrangeUpdater.get();
        }
        // skip if still no keyrange is available
        if (keyrange == null) {
            return "No servers available";
        }
        InetSocketAddress responsibleServer = keyrange.getSuccessor(key);
        try {
            Subscriber sub = subscribers.get(responsibleServer);
            if (sub == null) {
                sub = new Subscriber(responsibleServer, updateHandler, outputHandler);
                subscribers.put(responsibleServer, sub);
            }
            logger.info("Subscribe/unsubscribe action for key " + key + " on " + responsibleServer.toString());
            action.accept(sub);
            return "sent request";
        } catch (IOException e) {
            logger.warning("Failed to connect", e);
            return "Failed to connect - " + e.getMessage();
        }
    }

    public String subscribe(String key) {
        if (subscribedKeys.contains(key)) {
            return "Already subscribed";
        }
        return subscribeOrUnsubscribe(key, subscriber -> {
            subscriber.subscribe(key);
            subscribedKeys.add(key);
        });
    }

    public String unsubscribe(String key) {
        if (!subscribedKeys.contains(key)) {
            return "Not subscribed";
        }
        return subscribeOrUnsubscribe(key, subscriber -> {
            subscriber.unsubscribe(key);
            subscribedKeys.remove(key);
        });
    }

}
