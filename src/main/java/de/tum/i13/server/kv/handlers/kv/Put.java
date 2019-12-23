package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

import java.io.IOException;

public class Put implements Handler {

    public static final Log logger = new Log(Put.class);

    private final KVServer kvServer;

    public Put(KVServer server) {
        this.kvServer = server;
    }

    private void writeError(MessageWriter messageWriter, Message request, String key) {
        Message response = Message.getResponse(request);
        response.setCommand("put_error");
        response.put("key", key);
        response.put("msg", "internal server error");
        messageWriter.write(response);
        messageWriter.flush();
    }

    private void writeSuccess(MessageWriter messageWriter, Message request, String result, KVItem kvItem) {
        Message response = Message.getResponse(request);
        response.setCommand("put_" + result);
        response.put("key", kvItem.getKey());
        messageWriter.write(response);
        messageWriter.flush();
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {

        String key = message.get("key");
        String value = message.get("value");
        KVItem item = new KVItem(key, value);

        try {
            String result = kvServer.put(item, true);
            writeSuccess(messageWriter, message, result, item);
        } catch (IOException e) {
            logger.severe("Could not put value to Database", e);
            writeError(messageWriter, message, key);
        }

    }
}
