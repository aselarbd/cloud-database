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
        InetSocketAddress src = new InetSocketAddress(message.getSrc().getHostString(), Integer.parseInt(message.get("ecsport")));

        boolean serverRemains = ssm.getByECSAddress(src) != null;

        if (serverRemains) {
            Message releaseLock = Message.getResponse(message);
            releaseLock.setCommand("release_lock");
            messageWriter.write(releaseLock);
        }

        Message keyRange = new Message("keyrange");
        keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());
        ssm.broadcast(keyRange);

        if (!serverRemains) {
            Message bye = new Message("bye");
            messageWriter.write(bye);
        }
    }
}
