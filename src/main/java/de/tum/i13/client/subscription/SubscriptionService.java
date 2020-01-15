package de.tum.i13.client.subscription;

import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;
import de.tum.i13.shared.TaskRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SubscriptionService {

    TaskRunner taskRunner = new TaskRunner();

    private static final Log logger = new Log(SubscriptionService.class);
    private static final int RETRY_WAIT = 2000;

    private boolean reloadOngoing = false;
    private Map<InetSocketAddress, Subscriber> subscribers = new HashMap<>();
    private Map<String, List<InetSocketAddress>> subscribedKeys = new HashMap<>();
    private Set<String> retries = new HashSet<>();
    private ConsistentHashMap keyrange = null;
    private Supplier<ConsistentHashMap> keyrangeUpdater;
    private Consumer<KVItem> updateHandler;
    private Consumer<String> outputHandler;
    // lock for keyrange changes (the object itself is thread-safe, but it is set to null occasionally)
    private Lock keyrangeLock = new ReentrantLock();

    public SubscriptionService(Supplier<ConsistentHashMap> keyrangeUpdater, Consumer<KVItem> updateHandler,
                               Consumer<String> outputHandler) {
        this.keyrangeUpdater = keyrangeUpdater;
        this.updateHandler = updateHandler;
        this.outputHandler = outputHandler;
    }

    private void appendSubscription(String key, InetSocketAddress addr) {
        List<InetSocketAddress> addrs = subscribedKeys.computeIfAbsent(key, k -> new ArrayList<>());
        addrs.add(addr);
    }

    private void removeSubscription(String key, InetSocketAddress addr) {
        List<InetSocketAddress> addrs = subscribedKeys.get(key);
        if (addrs != null) {
            addrs.remove(addr);
            if (addrs.isEmpty()) {
                subscribedKeys.remove(key);
            }
        }
    }

    private Subscriber getSubscriber(InetSocketAddress addr) throws IOException {
        Subscriber sub = subscribers.get(addr);
        if (sub == null) {
            sub = new Subscriber(addr, updateHandler, this::subscriberEventHandler);
            subscribers.put(addr, sub);
        }
        return sub;
    }

    private void renewKeyrange() {
        keyrangeLock.lock();
        if (keyrange == null) {
            logger.fine("Reloading keyranges for subscriber");
            keyrange = keyrangeUpdater.get();
        }
        // skip if still no keyrange is available
        if (keyrange == null) {
            keyrangeLock.unlock();
            return;
        }
        // we need to check all subscription servers as topology might have changed
        subscribedKeys.forEach((key, addrs) -> {
            List<InetSocketAddress> newSuccessors = keyrange.getAllSuccessors(key);
            Set<InetSocketAddress> newAddrs = new HashSet<>(newSuccessors);
            newAddrs.removeAll(addrs);

            // send subscription to all new servers
            for (InetSocketAddress addr : newAddrs) {
                try {
                    Subscriber s = getSubscriber(addr);
                    s.subscribe(key);
                    addrs.add(addr);
                    logger.fine("Also subscribing to " + addr + " for key " + key);
                } catch (IOException e) {
                    logger.warning("Failed to move subscription of " + key
                        + " to " + addr.toString(), e);
                }
            }

            // remove all old subscribers. Unsubscription messages are not necessary as these servers are
            // not responsible anymore.
            addrs.retainAll(newSuccessors);
        });
        keyrangeLock.unlock();
    }

    private String subscribeOrUnsubscribe(String key, boolean isSubscription) {
        renewKeyrange();
        keyrangeLock.lock();
        // skip if still no keyrange is available
        if (keyrange == null) {
            keyrangeLock.unlock();
            return "No servers available";
        }
        List<InetSocketAddress> responsibleServers = keyrange.getAllSuccessors(key);
        String status = "";
        for (InetSocketAddress responsibleServer : responsibleServers) {
            try {
                Subscriber sub = getSubscriber(responsibleServer);
                if (isSubscription) {
                    sub.subscribe(key);
                    appendSubscription(key, responsibleServer);
                    logger.info("Subscription to key " + key + " on " + responsibleServer.toString());
                } else {
                    sub.unsubscribe(key);
                    removeSubscription(key, responsibleServer);
                    logger.info("Unsubscription from key " + key + " on " + responsibleServer.toString());
                }
            } catch (IOException e) {
                logger.warning("Failed to connect", e);
                status += "\nFailed to connect - " + e.getMessage();
            }
        }
        keyrangeLock.unlock();
        return "sent requests" + status;
    }

    private void reload() {
        if (reloadOngoing) {
            return;
        }
        reloadOngoing = true;
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
            renewKeyrange();
            reloadOngoing = false;
            logger.fine("Done with reload");
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
                    if (subscribedKeys.getOrDefault(key, new ArrayList<>()).contains(event.getSource())) {
                        // failed subscribe - consider unsubscribed again
                        subscribedKeys.remove(key);
                    } else {
                        // failed unsubscribe - consider as still subscribed
                        appendSubscription(key, event.getSource());
                    }
                } else {
                    // try once to reload everything
                    retries.add(key);
                    reload();
                }
                break;
            case SERVER_DOWN:
                // try to remove subscriber and prevent keyrange modifications in the meantime
                keyrangeLock.lock();
                Subscriber sub = subscribers.get(event.getSource());
                if (sub != null) {
                    try {
                        sub.quit();
                    } catch (Exception e) {
                        logger.warning("Failed to quit subscriber "
                                + event.getSource().toString(), e);
                    }
                    subscribers.remove(event.getSource());
                }
                keyrangeLock.unlock();
                reload();
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
        if (subscribedKeys.containsKey(key)) {
            return "Already subscribed";
        }
        // clear potential previous retries
        retries.remove(key);
        return subscribeOrUnsubscribe(key, true);
    }

    public String unsubscribe(String key) {
        if (!subscribedKeys.containsKey(key)) {
            return "Not subscribed";
        }
        // clear potential previous retries
        retries.remove(key);
        return subscribeOrUnsubscribe(key, false);
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
