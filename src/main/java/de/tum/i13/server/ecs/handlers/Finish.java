package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.ServerStateMap;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public class Finish implements BiConsumer<MessageWriter, Message> {

    private final ServerStateMap ssm;

    public Finish(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        InetSocketAddress src = new InetSocketAddress(message.get("ecsip"), Integer.parseInt(message.get("ecsport")));

        boolean serverRemains = ssm.getByECSAddress(src) != null;

        if (serverRemains) {
            Message releaseLock = new Message(Message.Type.RESPONSE, "release_lock");
            messageWriter.write(releaseLock);
        }

        Message keyRange = new Message(Message.Type.RESPONSE, "keyrange");
        keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());
        ssm.broadcast(keyRange);

        if (!serverRemains) {
            Message bye = new Message("bye");
            messageWriter.write(bye);
        }
    }
}
