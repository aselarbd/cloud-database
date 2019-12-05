package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public class Shutdown implements BiConsumer<MessageWriter, Message> {

    private final ServerStateMap ssm;

    public Shutdown(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        InetSocketAddress src = new InetSocketAddress(message.get("ecsip"), Integer.parseInt(message.get("ecsport")));
        ServerState server = ssm.getByECSAddress(src);
        ssm.remove(server);

        Message write_lock = new Message(Message.Type.RESPONSE, "write_lock");
        messageWriter.write(write_lock);

        Message keyRange = new Message(Message.Type.RESPONSE, "keyrange");
        keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());

        ServerState kvSuccessor = ssm.getKVSuccessor(server);
        kvSuccessor.getMessageWriter().write(keyRange);
        messageWriter.write(keyRange);
    }
}
