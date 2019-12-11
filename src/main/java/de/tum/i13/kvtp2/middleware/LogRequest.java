package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class LogRequest implements HandlerWrapper {

    private final Logger logger;

    public LogRequest(Logger logger) {
        this.logger = logger;
    }

    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (w, m) -> {
            logger.info("request: \n" + m.toString());
            next.accept(w, m);
        };
    }
}
