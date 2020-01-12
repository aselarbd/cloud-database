package de.tum.i13.client.subscription;

import de.tum.i13.client.subscription.SubscriberEvent.EventType;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.NonBlockingKVTP2Client;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Background runner which manages subscriptions to a specific server.
 */
public class Subscriber {
    private final static Log logger = new Log(Subscriber.class);
    private final static int HEALTH_INTVL = 10000;
    private int pendingHealthReplies = 0;
    private final InetSocketAddress addr;
    Consumer<KVItem> updateCallback;
    Consumer<SubscriberEvent> eventHandler;
    NonBlockingKVTP2Client client;

    /**
     * Creates a new instance subscribing to the given server.
     *
     * @param addr Server address
     * @param updateCallback A function taking a KVItem as argument. This gets called when a subscribed key
     *                       changes. It will be executed in the runner's thread, so you might need to take care
     *                       if you want to execute logic in a specific thread.
     * @param eventHandler A function which is called when an event happens which needs some management service
     *                     action (server down, server not responsible, error, ...)
     * @throws IOException If the connection to the server fails
     */
    public Subscriber(InetSocketAddress addr, Consumer<KVItem> updateCallback,
                      Consumer<SubscriberEvent> eventHandler) throws IOException {
        this.client = new NonBlockingKVTP2Client();
        this.addr = addr;
        this.updateCallback = updateCallback;
        this.eventHandler = eventHandler;
        // setup client
        this.client.setDefaultHandler(this::messageHandler);
        Future<Boolean> connected = this.client.connect(addr);
        // start client service
        ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
        clientExecutor.submit(() -> {
            try {
                this.client.start();
            } catch (IOException e) {
                logger.warning("Failed to start subscriber client", e);
            } catch (Exception e) {
                // log whatever goes wrong
                logger.warning("Subscriber client crashed", e);
                eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_DOWN, "executor"));
            }
        });
        // await connection establishment
        try {
            connected.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Could not connect", e);
        }
        // periodically check if server is alive
        ExecutorService alivenessChecker = Executors.newSingleThreadExecutor();
        alivenessChecker.submit(() -> {
            try {
                while(true) {
                    Thread.sleep(HEALTH_INTVL);
                    if (pendingHealthReplies > 1) {
                        logger.info("Health checker unresponsive");
                        eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_DOWN, "health"));
                        return;
                    }
                    pendingHealthReplies++;
                    client.send(new Message("health"));
                }
            } catch (Exception e) {
                logger.info("Health checker failed", e);
                eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_DOWN, "health"));
            }
        });
    }

    /**
     * Subscribe to updates of the given key.
     *
     * @param key Key to subscribe.
     */
    public void subscribe(String key) {
        try {
            client.send(msgWithKey("subscribe", key));
        } catch (RuntimeException e) {
            logger.info("Failed to send subscribe", e);
            eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_DOWN, "subscribe"));
        }
    }

    /**
     * Stop receiving updates of the given key.
     *
     * @param key Key to unsubscribe.
     */
    public void unsubscribe(String key) {
        try {
            client.send(msgWithKey("unsubscribe", key));
        } catch (RuntimeException e) {
            logger.info("Failed to send unsubscribe", e);
            eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_DOWN, "unsubscribe"));
        }
    }

    private Message msgWithKey(String cmd, String key) {
        Message msg = new Message(cmd);
        msg.put("key", key);
        return msg;
    }

    private void messageHandler(MessageWriter writer, Message message) {
        if (message == null) {
            logger.info("Got empty message at " + addr.toString());
            return;
        }
        switch (message.getCommand()) {
            case "pubsub_update":
                KVResult res = new KVResult(message);
                try {
                    res = res.decoded();
                } catch (IllegalArgumentException e) {
                    logger.info("could not decode value for " + message, e);
                }
                updateCallback.accept(res.getItem());
                break;
            case "server_not_responsible_for":
                String key = message.get("key");
                if (key == null) {
                    eventHandler.accept(new SubscriberEvent(addr, EventType.OTHER,
                            "Invalid message " + message.toString()));
                } else {
                    eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_NOT_RESPONSIBLE, key));
                }
                break;
            case "health":
                pendingHealthReplies--;
                String stopped = message.get("server_stopped");
                if (stopped == null) {
                    eventHandler.accept(new SubscriberEvent(addr, EventType.OTHER,
                            "Invalid message " + message.toString()));
                } else if (stopped.equals("true")){
                    eventHandler.accept(new SubscriberEvent(addr, EventType.SERVER_NOT_READY,""));
                }
                break;
            case "error":
                eventHandler.accept(new SubscriberEvent(addr, EventType.OTHER,
                        "KV Server does not support PubSub extension"));
                break;
            default:
                logger.info("Got unexpected message " + message.toString());
        }
    }
}
