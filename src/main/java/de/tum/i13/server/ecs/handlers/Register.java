package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;
import de.tum.i13.shared.HeartbeatSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class Register implements BiConsumer<MessageWriter, Message> {

    public static Logger logger = Logger.getLogger(Register.class.getName());

    private ServerStateMap ssm;

    public Register(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message msg) {
        InetSocketAddress kvAddr = new InetSocketAddress(msg.get("kvip"), Integer.parseInt(msg.get("kvport")));
        InetSocketAddress ecsAddr = new InetSocketAddress(msg.get("ecsip"), Integer.parseInt(msg.get("ecsport")));

        ServerState serverState = null;
        try {
            serverState = new ServerState(ecsAddr, kvAddr);
        } catch (IOException e) {
            logger.warning("Failed to register new server, aborting: " + kvAddr + " : " + e.getMessage());
            Message errorResponse = Message.getResponse(msg);
            errorResponse.setCommand("error");
            errorResponse.put("error", "could not connect to ecs API at " + ecsAddr);
            messageWriter.write(errorResponse);
            return;
        }
        ssm.add(serverState);
        heartbeat(serverState);

        String keyrangeString = ssm.getKeyRanges().getKeyrangeString();
        Message response = Message.getResponse(msg);
        response.setCommand("keyrange");
        response.put("keyrange", keyrangeString);

        ServerState kvPredecessor = ssm.getKVPredecessor(serverState);
        if (!kvPredecessor.getKV().equals(kvAddr)) {
            Message writeLock = new Message("lock");
            writeLock.put("lock", "true");
            Message keyRange = new Message("keyrange");
            keyRange.put("keyrange", keyrangeString);

            kvPredecessor.getClient().send(writeLock, (w, m) -> {
                logger.info("successfully locked server " + kvPredecessor.getKV());
            });
            kvPredecessor.getClient().send(keyRange, (w, m) -> {
                logger.info("successfully set keyrange for " + kvPredecessor.getKV());
            });
        }

        messageWriter.write(response);
    }

    private void heartbeat(ServerState receiver) {
        HeartbeatSender heartbeatSender = new HeartbeatSender(receiver.getKV());
        ScheduledExecutorService heartBeatService = heartbeatSender.start(() -> {
            ssm.remove(receiver);
            Message keyRange = new Message("keyrange");
            keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());
            ssm.broadcast(keyRange);
        });
        receiver.addShutdownHook(heartBeatService::shutdown);
    }

}
