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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SubscriptionService {
    private static final Log logger = new Log(SubscriptionService.class);
    private static final int RETRY_WAIT = 2000;
    Map<InetSocketAddress, Subscriber> subscribers = new HashMap<>();
    Set<String> subscribedKeys = new HashSet<>();
    Set<String> retries = new HashSet<>();
    ConsistentHashMap keyrange = null;
    Supplier<ConsistentHashMap> keyrangeUpdater;
    Consumer<KVItem> updateHandler;
    Consumer<String> outputHandler;
    // lock for keyrange changes (the object itself is thread-safe, but it is set to null occasionally)
    private Lock keyrangeLock = new ReentrantLock();

    public SubscriptionService(Supplier<ConsistentHashMap> keyrangeUpdater, Consumer<KVItem> updateHandler,
                               Consumer<String> outputHandler) {
        this.keyrangeUpdater = keyrangeUpdater;
        this.updateHandler = updateHandler;
        this.outputHandler = outputHandler;
    }

    private String subscribeOrUnsubscribe(String key, Consumer<Subscriber> action) {
        keyrangeLock.lock();
        if (keyrange == null) {
            logger.fine("Reloading keyranges for subscriber");
            keyrange = keyrangeUpdater.get();
        }
        // skip if still no keyrange is available
        if (keyrange == null) {
            keyrangeLock.unlock();
            return "No servers available";
        }
        InetSocketAddress responsibleServer = keyrange.getSuccessor(key);
        try {
            Subscriber sub = subscribers.get(responsibleServer);
            if (sub == null) {
                sub = new Subscriber(responsibleServer, updateHandler, this::subscriberEventHandler);
                subscribers.put(responsibleServer, sub);
            }
            logger.info("Subscribe/unsubscribe action for key " + key + " on " + responsibleServer.toString());
            action.accept(sub);
            return "sent request";
        } catch (IOException e) {
            logger.warning("Failed to connect", e);
            return "Failed to connect - " + e.getMessage();
        } finally {
            keyrangeLock.unlock();
        }
    }

    private void retry(String key) {
        // run a separate thread for retry as it might take longer, so the receive logic doesn't get blocked
        ExecutorService retryExec = Executors.newSingleThreadExecutor();
        retryExec.submit(() -> {
            // give servers some time to rebalance
            try {
                Thread.sleep(RETRY_WAIT);
            } catch (InterruptedException e) {
                logger.info("Interrupted while waiting for retry", e);
            }
            // force reloading of keyrange
            keyrangeLock.lock();
            keyrange = null;
            keyrangeLock.unlock();
            if (subscribedKeys.contains(key)) {
                subscribeOrUnsubscribe(key, subscriber -> subscriber.subscribe(key));
            } else {
                subscribeOrUnsubscribe(key, subscriber -> subscriber.unsubscribe(key));
            }
            logger.fine("Sent retry for " + key);
        });
    }

    private void subscriberEventHandler(SubscriberEvent event) {
        switch(event.getType()) {
            case SERVER_NOT_RESPONSIBLE:
                final String key = event.getMessage();
                logger.info(event.getSource().toString() + " not responsible for " + key);
                if (retries.contains(key)) {
                    outputHandler.accept("Failed to determine responsible server for " + key);
                    logger.info("Retry failed for " + key);
                    retries.remove(key);
                } else {
                    retries.add(key);
                    retry(key);
                }
                break;
            case SERVER_DOWN:
                break;
            case SERVER_NOT_READY:
                // TODO determine possibly pending actions and retry
                break;
            case OTHER:
            default:
                outputHandler.accept(event.getMessage());
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
