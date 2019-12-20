package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.KVServer;

import java.util.function.BiConsumer;

public class SetLockHandler implements Handler {

    private final KVServer kvServer;

    public SetLockHandler(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        this.kvServer.setLocked(message.get("lock").equals("true"));
        Message response = Message.getResponse(message);
        response.setCommand("ok");
        messageWriter.write(response);
    }
}
