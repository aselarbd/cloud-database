package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.pubsub.SubscriptionService;
import de.tum.i13.shared.KVItem;

public class CancelNotification implements Handler {

    private SubscriptionService subscriptionService;

    public CancelNotification(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        String key = message.get("key");
        String value = message.get("value");
        KVItem kvItem = new KVItem(key, value);
        subscriptionService.cancelNotification(kvItem);
    }
}
