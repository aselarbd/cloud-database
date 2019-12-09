package de.tum.i13.server.kv.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.ECSClientProcessor;
import de.tum.i13.server.kv.KVCache;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Get implements BiConsumer<MessageWriter, Message> {

    public static Logger logger = Logger.getLogger(Get.class.getName());

    private final KVCache kvCache;
    private final KVStore kvStore;

    public Get(KVCache kvCache, KVStore kvStore) {
        this.kvCache = kvCache;
        this.kvStore = kvStore;
    }

    private void writeFound(MessageWriter messageWriter, Message request, KVItem kvItem) {
        Message response = Message.getResponse(request);
        response.put("key", kvItem.getKey());
        response.put("value", kvItem.getValue());
        messageWriter.write(response);
        messageWriter.flush();
    }

    private void writeError(MessageWriter messageWriter, Message request, String key, String msg) {
        Message response = Message.getResponse(request);
        response.setCommand("get_error");
        response.put("key", key);
        response.put("msg", msg);
        messageWriter.write(response);
        messageWriter.flush();
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        String key = message.get("key");
        KVItem kvItem = kvCache.get(key);
        if (kvItem != null) {
            writeFound(messageWriter, message, kvItem);
            return;
        }

        KVItem result;
        try {
            result = kvStore.get(key);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not get value from Database", e);
            return;
        } finally {
            writeError(messageWriter, message, key, "Internal server error");
        }

        if (result != null) {
            writeFound(messageWriter, message, new KVItem(key, result.getValue()));
            return;
        }

        writeError(messageWriter, message, key, "not found");
    }
}
