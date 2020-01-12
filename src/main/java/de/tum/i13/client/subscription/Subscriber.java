package de.tum.i13.client.subscription;

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
    Consumer<KVItem> updateCallback;
    Consumer<String> errorCallback;
    NonBlockingKVTP2Client client;

    /**
     * Creates a new instance subscribing to the given server.
     *
     * @param addr Server address
     * @param updateCallback A function taking a KVItem as argument. This gets called when a subscribed key
     *                       changes. It will be executed in the runner's thread, so you might need to take care
     *                       if you want to execute logic in a specific thread.
     * @param errorCallback A function which is called when an error is received (server not responsible,
     *                      unknown message, ...)
     * @throws IOException If the connection to the server fails
     */
    public Subscriber(InetSocketAddress addr, Consumer<KVItem> updateCallback,
                      Consumer<String> errorCallback) throws IOException {
        this.client = new NonBlockingKVTP2Client();
        this.updateCallback = updateCallback;
        this.errorCallback = errorCallback;
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
            }
        });
        // await connection establishment
        try {
            connected.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Could not connect", e);
        }
    }

    /**
     * Subscribe to updates of the given key.
     *
     * @param key Key to subscribe.
     */
    public void subscribe(String key) {
        client.send(msgWithKey("subscribe", key));
    }

    /**
     * Stop receiving updates of the given key.
     *
     * @param key Key to unsubscribe.
     */
    public void unsubscribe(String key) {
        client.send(msgWithKey("unsubscribe", key));
    }

    private Message msgWithKey(String cmd, String key) {
        Message msg = new Message(cmd);
        msg.put("key", key);
        return msg;
    }

    private void messageHandler(MessageWriter writer, Message message) {
        if (message != null && message.getCommand().equals("pubsub_update")) {
            KVResult res = new KVResult(message);
            try {
                res = res.decoded();
            } catch (IllegalArgumentException e) {
                logger.info("could not decode value for " + message, e);
            }
            updateCallback.accept(res.getItem());
        } else if (message != null && message.getCommand().equals("server_not_responsible_for")) {
            String key = message.get("key") != null ? " " + message.get("key") : "";
            errorCallback.accept("server_not_responsible_for" + key);
        }
    }
}
