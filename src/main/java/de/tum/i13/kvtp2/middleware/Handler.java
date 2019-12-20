package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface Handler {

    void handle(MessageWriter writer, Message message);

    default Handler next(Handler next) {
        return (w, m) -> {
            handle(w, m);
            next.handle(w, m);
        };
    }
}
