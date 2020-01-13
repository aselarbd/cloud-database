package de.tum.i13.server.kv.handlers.ecs;

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
        String value = message.get("value");
        KVItem item = new KVItem(key, value);
        if (message.getCommand().equals("delete")) {
            item.setValue(Constants.DELETE_MARKER);
        }
        subscriptionService.replicateNotification(item);
    }
}
