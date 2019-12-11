package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public class ResponsibilityHandler {

    private InetSocketAddress kvAddress;
    private KeyRange keyRangeHandler;

    public ResponsibilityHandler(KeyRange keyRange, InetSocketAddress kvAddress) {
        this.keyRangeHandler = keyRange;
        this.kvAddress = kvAddress;
    }

    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (w, m) -> {
            if (m.getCommand().matches("put|delete|get") &&
                !keyRangeHandler.getKeyRange().getSuccessor(m.get("key")).equals(kvAddress)) {

                Message notResponsibleMessage = Message.getResponse(m);
                notResponsibleMessage.setCommand("server_not_responsible");
                w.write(notResponsibleMessage);
                w.flush();
            } else {
                next.accept(w, m);
            }
        };
    }
}
