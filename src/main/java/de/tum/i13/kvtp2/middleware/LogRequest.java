package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class LogRequest implements Handler {

    private final Logger logger;

    public LogRequest(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        logger.info("request: \n" + message.toString());
    }
}
