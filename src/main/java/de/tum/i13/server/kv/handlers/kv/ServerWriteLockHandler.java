package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;

public class ServerWriteLockHandler {

    private boolean locked;

    public ServerWriteLockHandler() {
        this.locked = false;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (w, m) -> {
            if (locked) {
                Message writeLock = Message.getResponse(m);
                m.setCommand("server_write_lock");
                w.write(writeLock);
                w.flush();
            } else {
                next.accept(w, m);
            }
        };
    }
}