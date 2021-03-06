package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.KVTP2ClientFactory;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;
import de.tum.i13.shared.HeartbeatSender;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;

public class Register implements Handler {

    private static final boolean DEBUG = false;
    public static final Log logger = new Log(Register.class);
    private final KVTP2ClientFactory clientFactory;

    private final ServerStateMap ssm;

    public Register(ServerStateMap ssm) {
        this(ssm, KVTP2Client::new);
    }

    public Register(ServerStateMap ssm, KVTP2ClientFactory clientFactory) {
        this.ssm = ssm;
        this.clientFactory = clientFactory;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message msg) {
        String remoteHostString = msg.getSrc().getHostString();
        InetSocketAddress kvAddr = new InetSocketAddress(remoteHostString, Integer.parseInt(msg.get("kvport")));
        InetSocketAddress ecsAddr = new InetSocketAddress(remoteHostString, Integer.parseInt(msg.get("ecsport")));

        ServerState serverState;
        try {
            KVTP2Client client = clientFactory.get(ecsAddr.getHostString(), ecsAddr.getPort());
            client.connect();
            serverState = new ServerState(ecsAddr, kvAddr, client);
        } catch (IOException e) {
            logger.warning("Failed to register new server, aborting: " + kvAddr, e);
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
                logger.warning("failed to send write lock to predecessor at: " + rebalancer.getECS(), e);
            }
            try {
                Message r = rebalancer.getClient().send(keyRange);
                if (r.getCommand().equals("error")) {
                    logger.warning("failed to send keyrange to predecessor at: " + rebalancer.getECS() + " : " + r.get("msg"));
                }
            } catch (IOException e) {
                logger.warning("failed to send keyrange to predecessor at: " + rebalancer.getECS(), e);
            }
        }

        try {
            Message keyRange = new Message("keyrange");
            keyRange.put("keyrange", keyrangeString);
            serverState.getClient().send(keyRange);
            Message response = Message.getResponse(msg);
            response.setCommand("ok");
            response.put("ip", serverState.getKV().getHostString());
            messageWriter.write(response);
        } catch (IOException e) {
            logger.warning("failed to send keyrange to new server", e);
        }
        logger.info("succesfully registered new kvServer: " + kvAddr);
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
