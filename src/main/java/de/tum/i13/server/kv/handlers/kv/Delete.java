package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.KVCache;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Delete implements BiConsumer<MessageWriter, Message> {

    public static final Logger logger = Logger.getLogger(Delete.class.getName());

    private final KVCache kvCache;
    private final KVStore kvStore;

    public Delete(KVCache kvCache, KVStore kvStore) {
        this.kvCache = kvCache;
        this.kvStore = kvStore;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {

        String key = message.get("key");

        try {
            if (kvStore.get(key) != null) {
                KVItem kvItem = new KVItem(key, Constants.DELETE_MARKER);
                kvStore.put(kvItem);
                kvCache.delete(kvItem);
                writeSuccess(messageWriter, message, key);
                return;
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not delete value from database", e);
        }
        writeError(messageWriter, message, key);
    }

    private void writeError(MessageWriter messageWriter, Message request, String key) {
        Message response = Message.getResponse(request);
        response.setCommand("delete_success");
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
