package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;
import de.tum.i13.shared.ConsistentHashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class Shutdown implements Handler {

    public static final Logger logger = Logger.getLogger(Shutdown.class.getName());

    private final ServerStateMap ssm;

    public Shutdown(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        InetSocketAddress src = new InetSocketAddress(message.getSrc().getHostString(), Integer.parseInt(message.get("ecsport")));
        ServerState server = ssm.getByECSAddress(src);

        Message lock = Message.getResponse(message);
        lock.setCommand("lock");
        lock.put("lock", "true");
        try {
            server.getClient().send(lock);
        } catch (IOException e) {
            logger.warning("failed to lock server for shutdown" + e.getMessage());
        }

        Message keyRange = new Message("keyrange");
        ConsistentHashMap newKeyRange = ssm.getKeyRanges();
        newKeyRange.remove(server.getKV());
        keyRange.put("keyrange", newKeyRange.getKeyrangeString());

        ServerState kvSuccessor = ssm.getKVSuccessor(server);
        if (kvSuccessor != null) {
            try {
                kvSuccessor.getClient().send(keyRange);
            } catch (IOException e) {
                // TODO: What to do if the successor has gone away?
                logger.warning(e.getMessage());
            }
        } else {
            logger.warning("shutting down the last available KVServer");
        }

        try {
            Message shutdownKeyRange = new Message("shutdown_keyrange");
            shutdownKeyRange.put("keyrange", newKeyRange.getKeyrangeString());
            server.getClient().send(shutdownKeyRange);
        } catch (IOException e) {
            logger.warning("failed to set new keyrange to shutdown server: " + e.getMessage());
        }

        ssm.remove(server);

        Message response = Message.getResponse(message);
        response.setCommand("ok");
        messageWriter.write(response);
    }
}
