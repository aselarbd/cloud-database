package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;
import de.tum.i13.shared.ConsistentHashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class Shutdown implements BiConsumer<MessageWriter, Message> {

    public static final Logger logger = Logger.getLogger(Shutdown.class.getName());

    private final ServerStateMap ssm;

    public Shutdown(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        InetSocketAddress src = new InetSocketAddress(message.get("ecsip"), Integer.parseInt(message.get("ecsport")));
        ServerState server = ssm.getByECSAddress(src);
        ssm.remove(server);

        Message lock = Message.getResponse(message);
        lock.setCommand("lock");
        lock.put("lock", "true");
        try {
            server.getClient().send(lock);
        } catch (IOException e) {
            logger.warning("failed to lock server for shutdown" + e.getMessage());
        }

        Message keyRange = new Message("keyrange");
        keyRange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());

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
            Message shutdownKeyrange = new Message("shutdown_keyrange");
            shutdownKeyrange.put("keyrange", ssm.getKeyRanges().getKeyrangeString());
            server.getClient().send(shutdownKeyrange);
        } catch (IOException e) {
            logger.warning("failed to set new keyrange to shutdown server: " + e.getMessage());
        }

        Message response = Message.getResponse(message);
        response.setCommand("ok");
        messageWriter.write(response);
    }
}
