package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.shared.ConsistentHashMap;

public class KeyRange implements Handler {

    private ConsistentHashMap keyRange;

    public void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRange = keyRange;
    }

    public ConsistentHashMap getKeyRange() {
        return this.keyRange;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        Message keyrangeResponse = Message.getResponse(message);
        keyrangeResponse.setCommand("keyrange");
        keyrangeResponse.put("keyrange", keyRange.getKeyrangeString());
        messageWriter.write(keyrangeResponse);
    }
}
