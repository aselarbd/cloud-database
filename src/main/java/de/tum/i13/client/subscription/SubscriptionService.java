package de.tum.i13.client.subscription;

import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;
import de.tum.i13.shared.TaskRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SubscriptionService {

    TaskRunner taskRunner = new TaskRunner();

    private static final Log logger = new Log(SubscriptionService.class);
    private static final int RETRY_WAIT = 2000;
    Map<InetSocketAddress, Subscriber> subscribers = new HashMap<>();
    Map<String, InetSocketAddress> subscribedKeys = new HashMap<>();
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

    private String subscribeOrUnsubscribe(String key, BiConsumer<InetSocketAddress, Subscriber> action) {
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
        List<InetSocketAddress> responsibleServers = keyrange.getAllSuccessors(key);
        String status = "";
        for (InetSocketAddress responsibleServer : responsibleServers) {
            try {
                Subscriber sub = subscribers.get(responsibleServer);
                if (sub == null) {
                    sub = new Subscriber(responsibleServer, updateHandler, this::subscriberEventHandler);
                    subscribers.put(responsibleServer, sub);
                }
                logger.info("Subscribe/unsubscribe action for key " + key + " on " + responsibleServer.toString());
                action.accept(responsibleServer, sub);
            } catch (IOException e) {
                logger.warning("Failed to connect", e);
                status += "\nFailed to connect - " + e.getMessage();
            }
        }
        keyrangeLock.unlock();
        return "sent requests" + status;
    }

    private void retry(String key) {
        // run a separate thread for retry as it might take longer, so the receive logic doesn't get blocked
        taskRunner.run(() -> {
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
            // use state map to determine which action is to be retried
            if (subscribedKeys.keySet().contains(key)) {
                subscribeOrUnsubscribe(key, (addr, subscriber) -> subscriber.subscribe(key));
            } else {
                subscribeOrUnsubscribe(key, (addr, subscriber) -> subscriber.unsubscribe(key));
            }
            logger.fine("Sent retry for " + key);
        });
    }

    private void subscriberEventHandler(SubscriberEvent event) {
        switch(event.getType()) {
            case SERVER_NOT_RESPONSIBLE:
                final String key = event.getDescription();
                logger.info(event.getSource().toString() + " not responsible for " + key);
                if (retries.contains(key)) {
                    outputHandler.accept("Failed to determine responsible server for " + key);
                    logger.info("Retry failed for " + key);
                    // restore previous state
                    if (subscribedKeys.keySet().contains(key)) {
                        // failed subscribe - consider unsubscribed again
                        subscribedKeys.remove(key);
                    } else {
                        // failed subscribe - consider as still subscribed
                        subscribedKeys.put(key, event.getSource());
                    }
                } else {
                    retries.add(key);
                    retry(key);
                }
                break;
            case SERVER_DOWN:
                // TODO manage new responsibilities
                break;
            case SERVER_NOT_READY:
                // TODO determine possibly pending actions and retry
                break;
            case OTHER:
                // use default action for other events, fall-through intended
            default:
                outputHandler.accept(event.getDescription());
        }
    }

    public String subscribe(String key) {
        if (subscribedKeys.keySet().contains(key)) {
            return "Already subscribed";
        }
        // clear potential previous retries
        retries.remove(key);
        return subscribeOrUnsubscribe(key, (addr, subscriber) -> {
            subscribedKeys.put(key, addr);
            subscriber.subscribe(key);
        });
    }

    public String unsubscribe(String key) {
        if (!subscribedKeys.keySet().contains(key)) {
            return "Not subscribed";
        }
        // clear potential previous retries
        retries.remove(key);
        return subscribeOrUnsubscribe(key, (addr, subscriber) -> {
            subscribedKeys.remove(key);
            subscriber.unsubscribe(key);
        });
    }

    public void quit() throws InterruptedException {
        subscribers.forEach((inetSocketAddress, subscriber) -> {
            try {
                subscriber.quit();
            } catch (InterruptedException e) {
                logger.warning("Exception while quitting subscribers", e);
            }
        });
        taskRunner.shutdown();
    }

}
