package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.ECSClientProcessor;
import de.tum.i13.server.kv.KVMain;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.ECSMessage;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class KeyRange implements BiConsumer<MessageWriter, Message> {

    public static Logger logger = Logger.getLogger(KeyRange.class.getName());

    private KVServer kvServer;
    private ExecutorService transferService;

    public KeyRange(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        String keyRangeString = message.get("keyrange");
        ConsistentHashMap newKeyRange = ConsistentHashMap.fromKeyrangeString(keyRangeString);

        InetSocketAddress oldPredecessor = kvServer.getKeyRange().getPredecessor(kvServer.getAddress());
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
                KVTP2Client ecsClient = new KVTP2Client(kvServer.getECSAddress().getHostString(), kvServer.getECSAddress().getPort());// connect to ecs and get ecs address for newPredecessor
                Message KVToECSMsg = new Message("kv_to_ecs");
                KVToECSMsg.put("kvip", newPredecessor.getHostString());
                KVToECSMsg.put("kvport", Integer.toString(newPredecessor.getPort()));

                String predecessorIP = "";
                int predecessorPort = 0;
                try {
                    Message response = ecsClient.send(KVToECSMsg);
                    predecessorIP = response.get("ecsip");
                    predecessorPort = Integer.parseInt(response.get("ecsport"));
                } catch (IOException e) {
                    // TODO: Handle the error, maybe try again. Tell ecs?
                    logger.warning("Could not get ecs api address for kv server at " + newPredecessor);
                }

                KVTP2Client kvtp2Client = new KVTP2Client(predecessorIP, predecessorPort);

                ExecutorService transferExecutor = Executors.newSingleThreadExecutor();
                try {
                    List<Future<String>> futures = transferExecutor.invokeAll(
                            oldKeys.stream().map((k) -> (Callable<String>) () -> {
                                Message put = new Message("put");
                                KVItem item = null;
                                try {
                                    item = kvServer.getItem(k);
                                    put.put("Key", item.getKey());
                                    put.put("value", item.getValue());
                                    kvtp2Client.send(put);
                                } catch (IOException e) {
                                    logger.warning("could not put item to new predecessor: " + item);
                                }
                                return null;
                            }).collect(Collectors.toSet())
                    );
                    futures.parallelStream().forEach((f) -> {
                        try {
                            f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            logger.warning("failed to finish putting value to new predecessor: " + e.getMessage());
                        }
                    });
                } catch (InterruptedException e) {
                    logger.warning("interrupted while putting values to new predecessor");
                }

                transferExecutor.shutdown();
            });
        }
    }
}
