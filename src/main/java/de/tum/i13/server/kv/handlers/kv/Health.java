package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;

public class Health implements Handler {

    private ServerStoppedHandler serverStoppedHandler;
    private ServerWriteLockHandler serverWriteLockHandler;

    public Health(ServerStoppedHandler serverStoppedHandler, ServerWriteLockHandler serverWriteLockHandler) {
        this.serverStoppedHandler = serverStoppedHandler;
        this.serverWriteLockHandler = serverWriteLockHandler;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        Message response = Message.getResponse(message);
        response.put("status", "pass");
        response.put("server_stopped", serverStoppedHandler.getServerStopped() ? "true" : "false");
        response.put("server_write_lock", serverWriteLockHandler.getLocked() ? "true" : "false");
        writer.write(response);
    }
}
