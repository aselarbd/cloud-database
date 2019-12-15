package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.shared.ConsistentHashMap;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public class ResponsibilityHandler {

    private final InetSocketAddress kvAddress;
    private final KeyRangeRead keyRangeHandler;

    public ResponsibilityHandler(KeyRangeRead keyRangeReplica, InetSocketAddress kvAddress) {
        this.keyRangeHandler = keyRangeReplica;
        this.kvAddress = kvAddress;
    }

    private void replyNotResponsible(MessageWriter w, Message m) {
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

    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (w, m) -> {
            ConsistentHashMap keyRangeWithReplica = keyRangeHandler.getKeyRangeRead();
            if (m.get("key") == null || m.get("key").isEmpty()) {
                replyError(w, m, "no key given");
            } else if (m.getCommand().matches("put|delete") &&
                    !keyRangeWithReplica.getSuccessor(m.get("key")).equals(kvAddress)) {
               replyNotResponsible(w, m);
            } else if (m.getCommand().matches("get") &&
                        !keyRangeWithReplica.getAllSuccessors(m.get("key")).contains(kvAddress)) {
                replyNotResponsible(w, m);
            } else {
                next.accept(w, m);
            }
        };
    }
}
