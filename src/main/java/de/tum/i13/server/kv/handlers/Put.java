package de.tum.i13.server.kv.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.KVCache;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Put implements BiConsumer<MessageWriter, Message> {

    public static Logger logger = Logger.getLogger(Put.class.getName());

    private final KVCache kvCache;
    private final KVStore kvStore;

    public Put(KVCache kvCache, KVStore kvStore) {

        this.kvCache = kvCache;
        this.kvStore = kvStore;
    }

    private void writeError(MessageWriter messageWriter, Message request, String key, String msg) {
        Message response = Message.getResponse(request);
        response.setCommand("put_error");
        response.put("key", key);
        response.put("msg", msg);
        messageWriter.write(response);
        messageWriter.flush();
    }

    private void writeSuccess(MessageWriter messageWriter, Message request, String result, KVItem kvItem) {
        Message response = Message.getResponse(request);
        response.setCommand("put_" + result);
        response.put("key", kvItem.getKey());
        response.put("value", kvItem.getValue());
        messageWriter.write(response);
        messageWriter.flush();
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {

        String key = message.get("key");
        String value = message.get("value");
        KVItem item = new KVItem(key, value);

        String result = "";
        try {
            result = kvStore.put(item);
            kvCache.put(item);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not put value to Database", e);
            writeError(messageWriter, message, key, "internal server error");
            return;
        }

        writeSuccess(messageWriter, message, result, item);
    }
}
