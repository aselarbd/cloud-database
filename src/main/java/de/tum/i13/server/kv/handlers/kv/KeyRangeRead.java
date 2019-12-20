package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.shared.ConsistentHashMap;

import java.util.function.BiConsumer;

public class KeyRangeRead implements Handler {

    private ConsistentHashMap keyRangeRead;

    public void setKeyRangeRead(ConsistentHashMap keyRangeRead) {
        this.keyRangeRead = keyRangeRead;
    }

    public ConsistentHashMap getKeyRangeRead() {
        return this.keyRangeRead;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        Message keyRangeResponse = Message.getResponse(message);
        keyRangeResponse.setCommand("keyrange_read");
        keyRangeResponse.put("keyrange", keyRangeRead.getKeyrangeReadString());
        messageWriter.write(keyRangeResponse);
    }
}