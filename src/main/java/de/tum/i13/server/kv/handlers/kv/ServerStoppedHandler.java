package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.HandlerWrapper;

import java.util.function.BiConsumer;

public class ServerStoppedHandler implements HandlerWrapper {

    private boolean serverStopped;

    public ServerStoppedHandler() {
        this.serverStopped = true;
    }

    public void setServerStopped(boolean serverStopped) {
        this.serverStopped = serverStopped;
    }

    @Override
    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (w, m) -> {
            if (serverStopped) {
                Message response = Message.getResponse(m);
                response.setCommand("server_stopped");
                w.write(response);
                w.flush();
            } else {
                next.accept(w, m);
            }
        };
    }

    public boolean getServerStopped() {
        return serverStopped;
    }
}
