package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.shared.ConsistentHashMap;

import java.util.function.BiConsumer;

public class KeyRangeRead implements BiConsumer<MessageWriter, Message> {

    private ConsistentHashMap keyRangeRead;

    public void setKeyRangeRead(ConsistentHashMap keyRangeRead) {
        this.keyRangeRead = keyRangeRead;
    }

    public ConsistentHashMap getKeyRangeRead() {
        return this.keyRangeRead;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        Message keyrangeResponse = Message.getResponse(message);
        keyrangeResponse.setCommand("keyrange_read");
        keyrangeResponse.put("keyrange", keyRangeRead.getKeyrangeReadString());
        messageWriter.write(keyrangeResponse);
    }
}