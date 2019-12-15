package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.HandlerWrapper;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.InetSocketAddressTypeConverter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class ReplicationHandler implements HandlerWrapper {
    private static final Logger logger = Logger.getLogger(ReplicationHandler.class.getName());

    private InetSocketAddress kvAddress;
    private KVServer kvServer;
    private Set<InetSocketAddress> successors = new HashSet<>();
    private Map<String, KVTP2Client> replClients = new HashMap<>();
    private Map<String, ExecutorService> replRunners = new HashMap<>();

    public ReplicationHandler(KVServer kvServer, InetSocketAddress kvAddress) {
        this.kvServer = kvServer;
        this.kvAddress = kvAddress;
    }

    /**
     * Initialize a new KVTP2Client to a replica server.
     *
     * @param addr
     * @return
     */
    private KVTP2Client connectReplica(InetSocketAddress addr) {
        KVTP2Client ecsClient = null;
        final String hostStr = InetSocketAddressTypeConverter.addrString(addr);
        try {
            ecsClient = kvServer.getBlockingECSClient();
        } catch (IOException e) {
            logger.severe("failed to get ecs client: " + e.getMessage());
        }
        Message KVToECSMsg = new Message("kv_to_ecs");
        KVToECSMsg.put("kvip", addr.getHostString());
        KVToECSMsg.put("kvport", Integer.toString(addr.getPort()));

        String replIp = "";
        int replPort = 0;
        try {
            Message response = ecsClient.send(KVToECSMsg);
            replIp = response.get("ecsip");
            replPort = Integer.parseInt(response.get("ecsport"));
        } catch (IOException e) {
            // TODO: Handle the error, maybe try again. Tell ecs?
            logger.warning("Could not get ecs api address for kv server at " + addr);
        }
        KVTP2Client kvtp2Client = new KVTP2Client(replIp, replPort);
        replClients.put(hostStr, kvtp2Client);
        return kvtp2Client;
    }

    /**
     * Called when keyrange changes. Close all servers which are no more present and add new ones.
     *
     * @param newKeyrange
     */
    public void keyrangeUpdated(ConsistentHashMap newKeyrange) {
        // TODO some locking while successors are changed
        Set<InetSocketAddress> newSuccessors;
        if (newKeyrange.size() < 3) {
            // no replication.
            newSuccessors = new HashSet<>();
        } else {
            newSuccessors = new HashSet<>(newKeyrange.getAllSuccessors(kvAddress));
            newSuccessors.remove(kvAddress);
        }

        // close all old connectors
        Set<InetSocketAddress> oldServers = new HashSet<>(successors);
        oldServers.removeAll(newSuccessors);

        for (InetSocketAddress addr : oldServers) {
            final String hostStr = InetSocketAddressTypeConverter.addrString(addr);
            // TODO: shutdown kvtp client
            replRunners.get(hostStr).shutdown();
            replClients.remove(hostStr);
            replRunners.remove(hostStr);
        }

        // Initialize new connectors
        Set<InetSocketAddress> newServers = new HashSet<>(newSuccessors);
        newServers.removeAll(successors);

        for (InetSocketAddress addr : newServers) {
            final String hostStr = InetSocketAddressTypeConverter.addrString(addr);
            ExecutorService runner = Executors.newSingleThreadExecutor();
            replRunners.put(hostStr, runner);
        }

        successors = newSuccessors;
    }

    @Override
    public BiConsumer<MessageWriter, Message> wrap(BiConsumer<MessageWriter, Message> next) {
        return (messageWriter, message) -> {
            for (InetSocketAddress addr : successors) {
                final String hostStr = InetSocketAddressTypeConverter.addrString(addr);
                ExecutorService runner = replRunners.get(hostStr);

                runner.submit(() -> {
                    KVTP2Client kvtp2Client = replClients.get(hostStr);
                    if (kvtp2Client == null) {
                        kvtp2Client = connectReplica(addr);
                    }

                    Message storeRepl = new Message(message.getCommand());
                    storeRepl.put("key", message.get("key"));
                    storeRepl.put("value", message.get("value"));
                    try {
                        kvtp2Client.send(storeRepl);
                    } catch (IOException e) {
                        logger.warning("Failed to replicate " + message.toString() + " - " + e.getMessage());
                    }
                });
            }
            next.accept(messageWriter, message);
        };
    }
}
