package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.Log;

import java.io.IOException;

public class Delete implements Handler {

    public static final Log logger = new Log(Delete.class);

    private final KVServer kvServer;

    public Delete(KVServer server) {
        this.kvServer = server;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {

        String key = message.get("key");

        try {
            if (kvServer.delete(key)) {
                writeSuccess(messageWriter, message, key);
                return;
            }
        } catch (IOException e) {
            logger.severe("Could not delete value from database", e);
        }
        writeError(messageWriter, message, key);
    }

    private void writeError(MessageWriter messageWriter, Message request, String key) {
        Message response = Message.getResponse(request);
        response.setCommand("delete_error");
        response.put("key", key);
        writeAndFlush(messageWriter, response);
    }

    private void writeAndFlush(MessageWriter messageWriter, Message response) {
        messageWriter.write(response);
        messageWriter.flush();
    }

    private void writeSuccess(MessageWriter messageWriter, Message request, String key) {
        Message response = Message.getResponse(request);
        response.setCommand("delete_success");
        response.put("key", key);
        writeAndFlush(messageWriter, response);
    }
}
