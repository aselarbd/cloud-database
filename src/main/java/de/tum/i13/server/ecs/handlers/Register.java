package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;
import de.tum.i13.shared.HeartbeatSender;

import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

public class Register implements BiConsumer<MessageWriter, Message> {

    private ServerStateMap ssm;

    public Register(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message msg) {
        InetSocketAddress kvAddr = new InetSocketAddress(msg.get("kvip"), Integer.parseInt(msg.get("kvport")));
        InetSocketAddress ecsAddr = new InetSocketAddress(msg.get("ecsip"), Integer.parseInt(msg.get("ecsport")));

        ServerState serverState = new ServerState(ecsAddr, messageWriter, kvAddr);
        ssm.add(serverState);
        heartbeat(serverState);

        String keyrangeString = ssm.getKeyRanges().getKeyrangeString();
        Message response = new Message(Message.Type.RESPONSE, "keyrange");
        response.put("keyrange", keyrangeString);

        ServerState kvPredecessor = ssm.getKVPredecessor(serverState);
        if (!kvPredecessor.getKV().equals(kvAddr)) {
            Message writeLock = new Message(Message.Type.RESPONSE, "write_lock");
            Message keyRange = new Message(Message.Type.RESPONSE, "keyrange");
            keyRange.put("keyrange", keyrangeString);
            kvPredecessor.getMessageWriter().write(writeLock);
            kvPredecessor.getMessageWriter().write(keyRange);
        }

        messageWriter.write(response);
    }

    private void heartbeat(ServerState receveiver) {
        HeartbeatSender heartbeatSender = new HeartbeatSender(receveiver.getKV());
        ScheduledExecutorService heartBeatService = heartbeatSender.start(() -> {
            ssm.remove(receveiver);
            Message keyRange = new Message(Message.Type.RESPONSE, "keyrange");
            keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());
            ssm.broadcast(keyRange);
        });
        receveiver.addShutdownHook(heartBeatService::shutdown);
    }

}
