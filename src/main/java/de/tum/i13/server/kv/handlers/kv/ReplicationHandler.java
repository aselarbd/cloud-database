package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.HandlerWrapper;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.shared.KVItem;

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicationHandler implements HandlerWrapper {
    private static final Logger logger = Logger.getLogger(ReplicationHandler.class.getName());

    private Replicator replicator;

    public ReplicationHandler(Replicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (messageWriter, message) -> {
            try {
                replicator.replicate(new KVItem(message.get("key"), message.get("value")));
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "failed to replicate " + message.toString(), e);
            }
            next.accept(messageWriter, message);
        };
    }
}
