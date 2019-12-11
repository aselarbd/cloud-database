package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface HandlerWrapper {
    BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next);
}
