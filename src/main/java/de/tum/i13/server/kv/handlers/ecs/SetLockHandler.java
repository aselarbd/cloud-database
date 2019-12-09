package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.io.IOException;
import java.util.function.BiConsumer;

public class SetLockHandler implements BiConsumer<MessageWriter, Message> {

    private boolean locked;

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        locked = message.get("lock").equals("true");
    }
}
