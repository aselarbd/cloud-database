package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.pubsub.SubscriptionService;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

public class Publication implements Handler {

    public static final Log logger = new Log(Publication.class);

    private SubscriptionService subscriptionService;

    public Publication(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        logger.info("publish change for: " + message);

        String key = message.get("key");
        if (message.getCommand().equals("delete")) {
            KVItem item = new KVItem(key, Constants.DELETE_MARKER);
            subscriptionService.notify(item);
        } else if (message.getCommand().equals("put")) {
            String value = message.get("value");
            KVItem item = new KVItem(key, value);
            subscriptionService.notify(item);
        }
    }
}
