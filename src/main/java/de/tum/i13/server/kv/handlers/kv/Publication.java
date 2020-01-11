package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.shared.Log;

public class Publication implements Handler {

    public static final Log logger = new Log(Publication.class);

    @Override
    public void handle(MessageWriter writer, Message message) {
        logger.info("publish change for: " + message);
    }
}
