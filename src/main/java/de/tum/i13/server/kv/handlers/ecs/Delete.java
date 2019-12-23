package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.Log;

import java.io.IOException;

public class Delete implements Handler {

    public static final Log logger = new Log(Delete.class);

    private final KVServer kvServer;

    public Delete(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        String key = message.get("key");

        try {
            kvServer.delete(key);
        } catch (IOException e) {
            logger.warning("deletion failed", e);
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
