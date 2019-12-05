package de.tum.i13.server.kv.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;

public class Put implements BiConsumer<MessageWriter, Message> {
    @Override
    public void accept(MessageWriter messageWriter, Message message) {

    }
}
