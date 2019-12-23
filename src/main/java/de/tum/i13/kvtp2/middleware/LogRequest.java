package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.shared.Log;

public class LogRequest implements Handler {

    private final Log logger;

    public LogRequest(Log logger) {
        this.logger = logger;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        logger.info("request: \n" + message.toString());
    }
}
