package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.HandlerWrapper;

import java.util.function.BiConsumer;

public class ReplicationHandler implements HandlerWrapper {
    @Override
    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return next;
    }
}
