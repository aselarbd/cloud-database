package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Log;

import java.net.InetSocketAddress;

public class ResponsibilityHandler implements Handler {

    public static final Log logger = new Log(ResponsibilityHandler.class);

    private InetSocketAddress kvAddress;
    private final KeyRangeRead keyRangeHandler;

    public ResponsibilityHandler(KeyRangeRead keyRangeReplica) {
        this(keyRangeReplica, null);
    }

    public ResponsibilityHandler(KeyRangeRead keyRangeReplica, InetSocketAddress kvAddress) {
        this.keyRangeHandler = keyRangeReplica;
        this.kvAddress = kvAddress;
    }

    public void setKvAddress(InetSocketAddress kvAddress) {
        this.kvAddress = kvAddress;
    }

    @Override
    public void handle(MessageWriter w, Message m) {
        Message notResponsibleMessage = Message.getResponse(m);
        notResponsibleMessage.setCommand("server_not_responsible");
        w.write(notResponsibleMessage);
        w.flush();
    }

    private void replyError(MessageWriter w, Message m, String msg) {
        Message error = Message.getResponse(m);
        error.setCommand("error");
        error.put("msg", msg);
        w.write(error);
        w.flush();
    }

    @Override
    public Handler next(Handler next) {
        return (w, m) -> {
            ConsistentHashMap keyRangeWithReplica = keyRangeHandler.getKeyRangeRead();
            if (m.get("key") == null || m.get("key").isEmpty()) {
                logger.info("got request without key: " + m.toString());
                replyError(w, m, "no key given");
            } else if (m.getCommand().matches("put|delete") &&
                    !keyRangeWithReplica.getSuccessor(m.get("key")).equals(kvAddress)) {
                logger.info("request key out of keyrange: " + m.toString());
                handle(w, m);
            } else if (m.getCommand().matches("get") &&
                    !keyRangeWithReplica.getAllSuccessors(m.get("key")).contains(kvAddress)) {
                logger.info("request key out of keyrange: " + m.toString());
                handle(w, m);
            } else {
                next.handle(w, m);
            }
        };
    }
}
