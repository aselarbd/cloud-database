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
    NonBlockingKVTP2Client client;

    /**
     * Creates a new instance subscribing to the given server.
     *
     * @param addr Server address
     * @param updateCallback A function taking a KVItem as argument. This gets called when a subscribed key
     *                       changes. It will be executed in the runner's thread, so you might need to take care
     *                       if you want to execute logic in a specific thread.
     * @throws IOException If the connection to the server fails
     */
    public Subscriber(InetSocketAddress addr, Consumer<KVItem> updateCallback) throws IOException {
        this.client = new NonBlockingKVTP2Client();
        this.updateCallback = updateCallback;
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
        client.send(v1MsgWithKey("subscribe", key));
    }

    /**
     * Stop receiving updates of the given key.
     *
     * @param key Key to unsubscribe.
     */
    public void unsubscribe(String key) {
        client.send(v1MsgWithKey("unsubscribe", key));
    }

    private Message v1MsgWithKey(String cmd, String key) {
        Message msg = new Message(cmd);
        msg.setVersion(Message.Version.V1);
        msg.put("key", key);
        return msg;
    }

    private void messageHandler(MessageWriter writer, Message message) {
        if (message != null && message.getCommand().equals("put_update")) {
            KVResult res = new KVResult(message);
            updateCallback.accept(res.getItem());
        }
    }
}
