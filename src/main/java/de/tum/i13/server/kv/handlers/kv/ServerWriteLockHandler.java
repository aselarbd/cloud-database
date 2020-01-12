package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;

public class ServerWriteLockHandler implements Handler {

    private boolean locked;

    public ServerWriteLockHandler() {
        this.locked = false;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    @Override
    public Handler next(Handler next) {
        return (w, m) -> {
            if (locked) {
                handle(w, m);
            } else {
                next.handle(w, m);
            }
        };
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        Message writeLock = Message.getResponse(message);
        writeLock.setCommand("server_write_lock");
        writer.write(writeLock);
        writer.flush();
    }

    public boolean getLocked() {
        return locked;
    }
}
