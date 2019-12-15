package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.KVServer;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class Delete implements BiConsumer<MessageWriter, Message> {

    public static final Logger logger = Logger.getLogger(Put.class.getName());

    private final KVServer kvServer;

    public Delete(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        String key = message.get("key");

        try {
            kvServer.delete(key);
        } catch (IOException e) {
            logger.warning("deletion failed : " + e.getMessage());
            Message response = Message.getResponse(message);
            response.setCommand("error");
            response.put("msg", e.getMessage());
            messageWriter.write(response);
            return;
        }

        Message response = Message.getResponse(message);
        response.setCommand("ok");
        messageWriter.write(response);
    }
}
