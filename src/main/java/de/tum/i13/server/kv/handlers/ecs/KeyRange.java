package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class KeyRange implements Handler {

    public static final Log logger = new Log(KeyRange.class);

    private final KVServer kvServer;
    private ExecutorService transferService;

    private ConsistentHashMap nextKeyRange;

    public KeyRange(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        String keyRangeString = message.get("keyrange");
        ConsistentHashMap newKeyRange = ConsistentHashMap.fromKeyrangeString(keyRangeString);
        ConsistentHashMap oldKeyRange = nextKeyRange == null ? kvServer.getKeyRange() : nextKeyRange;

        InetSocketAddress oldPredecessor = oldKeyRange == null ? null : oldKeyRange.getPredecessor(kvServer.getAddress());
        InetSocketAddress newPredecessor = newKeyRange.getPredecessor(kvServer.getAddress());

        if (oldPredecessor != null && !oldPredecessor.equals(newPredecessor)) {
            Set<String> oldKeys = kvServer.getAllKeys((s) ->
                    !newKeyRange
                            .getSuccessor(s)
                            .equals(kvServer.getAddress())
            );
            if (transferService == null) {
                transferService = Executors.newSingleThreadExecutor();
            }

            transferService.submit(() -> {
                KVTP2Client ecsClient = null;
                try {
                    ecsClient = kvServer.getBlockingECSClient();
                } catch (IOException e) {
                    logger.severe("failed to get ecs client", e);
                }
                Message KVToECSMsg = new Message("kv_to_ecs");
                KVToECSMsg.put("kvip", newPredecessor.getHostString());
                KVToECSMsg.put("kvport", Integer.toString(newPredecessor.getPort()));

                String predecessorIP = "";
                int predecessorPort = 0;
                try {
                    Message KVToECSResponse = ecsClient.send(KVToECSMsg);
                    if (!KVToECSResponse.getCommand().equals("error")) {
                        predecessorIP = KVToECSResponse.get("ecsip");
                        predecessorPort = Integer.parseInt(KVToECSResponse.get("ecsport"));
                    } else {
                        logger.warning("Could not get ecs api address for kv server at " + newPredecessor);
                    }
                } catch (IOException e) {
                    // TODO: Handle the error, maybe try again. Tell ecs?
                    logger.warning("Could not get ecs api address for kv server at " + newPredecessor, e);
                }

                KVTP2Client kvtp2Client = new KVTP2Client(predecessorIP, predecessorPort);
                ExecutorService transferExecutor = Executors.newSingleThreadExecutor();
                try {
                    kvtp2Client.connect();
                    List<Future<String>> futures = transferExecutor.invokeAll(
                            oldKeys.stream().map((k) -> (Callable<String>) () -> {
                                Message put = new Message("put");
                                KVItem item = null;
                                try {
                                    item = kvServer.getItem(k);
                                    put.put("key", item.getKey());
                                    put.put("value", item.getValue());
                                    kvtp2Client.send(put);
                                } catch (IOException e) {
                                    logger.warning("could not put item to new predecessor: " + item, e);
                                }
                                return null;
                            }).collect(Collectors.toSet())
                    );
                    futures.forEach((f) -> {
                        try {
                            f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            logger.warning("failed to finish putting value to new predecessor", e);
                        }
                    });
                    kvtp2Client.close();
                } catch (InterruptedException e) {
                    logger.warning("interrupted while putting values to new predecessor");
                } catch (IOException e) {
                    logger.warning("Failed to transfer elements", e);
                }

                Message finish = new Message("finish");
                finish.put("ecsip", kvServer.getControlAPIServerAddress().getHostString());
                finish.put("ecsport", Integer.toString(kvServer.getControlAPIServerAddress().getPort()));
                try {
                    Message res = ecsClient.send(finish);
                    if (res.getCommand().equals("release_lock")) {
                        kvServer.setLocked(false);
                    }
                } catch (IOException e) {
                    logger.warning("failed to send finish to ecs", e);
                }
            });
            nextKeyRange = newKeyRange;
        } else {
            Executors.newSingleThreadExecutor().submit(() -> {
                kvServer.setKeyRange(newKeyRange);
                nextKeyRange = null;
            });
        }
        Message response = Message.getResponse(message);
        response.setCommand("ok");
        messageWriter.write(response);
        kvServer.setStopped(false);
    }
}
