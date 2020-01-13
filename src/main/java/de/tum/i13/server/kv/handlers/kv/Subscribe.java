package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.pubsub.SubscriptionService;
import de.tum.i13.shared.Log;

public class Subscribe implements Handler {

    public static final Log logger = new Log(Subscribe.class);

    private SubscriptionService subscriptionService;

    public Subscribe(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }


    @Override
    public void handle(MessageWriter writer, Message message) {
        logger.info("subscribe: " + message);

        if (message.getCommand().equals("subscribe")) {
            if (message.get("replication") != null && message.get("replication").equals("true")) {
                subscriptionService.subscribeReplicatedClient(message.get("key"), message.getSrc(), writer);
            } else {
                subscriptionService.subscribe(message.get("key"), message.getSrc(), writer);
            }
        } else if (message.getCommand().equals("unsubscribe")) {
            subscriptionService.unsubscribe(message.get("key"), message.getSrc());
            subscriptionService.unsubscribeReplicatedClient(message.get("key"), message.getSrc());
        }
    }
}
