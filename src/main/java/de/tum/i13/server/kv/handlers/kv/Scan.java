package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.KVCache;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;
import java.util.Set;

public class Scan implements Handler {
    public static final Log logger = new Log(Get.class);

    private final KVCache kvCache;
    private final KVStore kvStore;

    public Scan(KVCache kvCache, KVStore kvStore) {
        this.kvCache = kvCache;
        this.kvStore = kvStore;
    }

    private String getKVSetAsString(Set<KVItem> kvItemSet){
        StringBuffer buffer = new StringBuffer("");
        for (KVItem item : kvItemSet){
            buffer.append(item.toString());
            buffer.append(",");
        }
        return buffer.substring(0, buffer.length() -1);
    }

    private void writeFound(MessageWriter messageWriter, Message request, String key, Set<KVItem> kvItemSet) {
        Message response = Message.getResponse(request);
        response.setCommand("scan_success");
        response.put("key", key);
        response.put("values", getKVSetAsString(kvItemSet));
        messageWriter.write(response);
        messageWriter.flush();
    }

    private void writeError(MessageWriter messageWriter, Message request, String key, String msg) {
        Message response = Message.getResponse(request);
        response.setCommand("scan_error");
        response.put("key", key);
        response.put("msg", msg);
        messageWriter.write(response);
        messageWriter.flush();
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        String partialKey = message.get("partialKey");
        Set<KVItem> kvItemSet = kvCache.scan(partialKey);
        if (kvItemSet.size() > 0) {
            writeFound(writer, message, partialKey, kvItemSet);
            return;
        }

    }

}
