package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.Server;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;
import de.tum.i13.shared.HeartbeatSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

public class Register implements BiConsumer<MessageWriter, Message> {

    private static final boolean DEBUG = false;
    public static Logger logger = Logger.getLogger(Register.class.getName());
    private final KVTP2ClientFactory clientFactory;

    private ServerStateMap ssm;

    public Register(ServerStateMap ssm) {
        this(ssm, KVTP2Client::new);
    }

    public Register(ServerStateMap ssm, KVTP2ClientFactory clientFactory) {
        this.ssm = ssm;
        this.clientFactory = clientFactory;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message msg) {
        InetSocketAddress kvAddr = new InetSocketAddress(msg.get("kvip"), Integer.parseInt(msg.get("kvport")));
        InetSocketAddress ecsAddr = new InetSocketAddress(msg.get("ecsip"), Integer.parseInt(msg.get("ecsport")));

        ServerState serverState = null;
        try {
            KVTP2Client client = clientFactory.get(ecsAddr.getHostString(), ecsAddr.getPort());
            client.connect();
            serverState = new ServerState(ecsAddr, kvAddr, client);
        } catch (IOException e) {
            logger.warning("Failed to register new server, aborting: " + kvAddr + " : " + e.getMessage());
            Message errorResponse = Message.getResponse(msg);
            errorResponse.setCommand("error");
            errorResponse.put("error", "could not connect to ecs API at " + ecsAddr);
            messageWriter.write(errorResponse);
            return;
        }

        // the server which would currently take keys up to the
        // hash of the address of the new server. Or: the one
        // which will potentially have to re-balance some keys
        ServerState rebalancer = ssm.getKVSuccessor(serverState);

        ssm.add(serverState);
        heartbeat(serverState);

        String keyrangeString = ssm.getKeyRanges().getKeyrangeString();
        Message response = Message.getResponse(msg);
        response.setCommand("keyrange");
        response.put("keyrange", keyrangeString);

        // no re-balancing if there's only one server
        if (ssm.getKeyRanges().size() > 1) {
            Message writeLock = new Message("lock");
            writeLock.put("lock", "true");
            Message keyRange = new Message("keyrange");
            keyRange.put("keyrange", keyrangeString);

            try {
                Message r = rebalancer.getClient().send(writeLock);
                if (r.getCommand().equals("error")) {
                    logger.warning("failed to send write lock to predecessor at: " + rebalancer.getECS() + " : " + r.get("msg"));
                }
            } catch (IOException e) {
                logger.warning("failed to send write lock to predecessor at: " + rebalancer.getECS() + " : " + e.getMessage());
            }
            try {
                Message r = rebalancer.getClient().send(keyRange);
                if (r.getCommand().equals("error")) {
                    logger.warning("failed to send keyrange to predecessor at: " + rebalancer.getECS() + " : " + r.get("msg"));
                }
            } catch (IOException e) {
                logger.warning("failed to send keyrange to predecessor at: " + rebalancer.getECS() + " : " + e.getMessage());
            }
        }

        logger.info("succesfully registered new kvServer: " + kvAddr);
        messageWriter.write(response);
    }

    private void heartbeat(ServerState receiver) {
        HeartbeatSender heartbeatSender = new HeartbeatSender(receiver.getKV());
        ScheduledExecutorService heartBeatService = heartbeatSender.start(() -> {
            if (!DEBUG) {
                ssm.remove(receiver);
                Message keyRange = new Message("keyrange");
                keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());
                ssm.broadcast(keyRange);
            }
        });
        receiver.addShutdownHook(heartBeatService::shutdown);
    }

}
