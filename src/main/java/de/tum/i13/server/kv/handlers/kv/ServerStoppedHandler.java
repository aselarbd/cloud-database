package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;

public class ServerStoppedHandler implements Handler {

    private boolean serverStopped;

    public ServerStoppedHandler() {
        this.serverStopped = true;
    }

    public void setServerStopped(boolean serverStopped) {
        this.serverStopped = serverStopped;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        Message response = Message.getResponse(message);
        response.setCommand("server_stopped");
        writer.write(response);
        writer.flush();
    }

    @Override
    public Handler next(Handler next) {
        return (w, m) -> {
            if (serverStopped) {
                handle(w, m);
            } else {
                next.handle(w, m);
            }
        };
    }

    public boolean getServerStopped() {
        return serverStopped;
    }
}
