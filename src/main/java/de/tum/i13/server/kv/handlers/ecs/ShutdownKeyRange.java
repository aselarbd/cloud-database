package de.tum.i13.server.kv.handlers.ecs;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.kv.KVServer;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ShutdownKeyRange implements BiConsumer<MessageWriter, Message> {


    public static final Logger logger = Logger.getLogger(KeyRange.class.getName());

    private final KVServer kvServer;

    public ShutdownKeyRange(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        String keyRangeString = message.get("keyrange");
        ConsistentHashMap newKeyRange = ConsistentHashMap.fromKeyrangeString(keyRangeString);

        KVTP2Client ecsClient = null;
        try {
            ecsClient = kvServer.getBlockingECSClient();
        } catch (IOException e) {
            logger.severe("failed to get ecs client: " + e.getMessage());
            return;
        }
        KVTP2Client finalEcsClient = ecsClient;

        Message response = Message.getResponse(message);
        response.setCommand("ok");
        messageWriter.write(response);
        messageWriter.flush();

        if (newKeyRange.size() <= 0) {
            ExecutorService finishExecutor = Executors.newSingleThreadExecutor();
            finishExecutor.submit(() -> {
                sendFinish(finalEcsClient);
            });
            return;
        }

        ExecutorService transferService = Executors.newSingleThreadExecutor();

        transferService.submit(() -> {
            Map<InetSocketAddress, KVTP2Client> clients = new HashMap<>();

            ExecutorService transferExecutor = Executors.newSingleThreadExecutor();
            try {
                List<Future<String>> futures = transferExecutor.invokeAll(
                        kvServer.getAllKeys((k) -> true).stream().map((k) -> (Callable<String>) () -> {
                            Message put = new Message("put");
                            KVItem item = null;
                            try {
                                item = kvServer.getItem(k);
                                put.put("Key", item.getKey());
                                put.put("value", item.getValue());
                                InetSocketAddress successor = newKeyRange.getSuccessor(item.getKey());

                                if (!clients.containsKey(successor)) {
                                    Message KVToECSMsg = new Message("kv_to_ecs");
                                    KVToECSMsg.put("kvip", successor.getHostString());
                                    KVToECSMsg.put("kvport", Integer.toString(successor.getPort()));
                                    Message kvToEcs = finalEcsClient.send(KVToECSMsg);
                                    String ip = kvToEcs.get("ecsip");
                                    int port = Integer.parseInt(kvToEcs.get("ecsport"));
                                    KVTP2Client kvtp2Client = new KVTP2Client(ip, port);
                                    clients.put(successor, kvtp2Client);
                                }

                                clients.get(successor).send(put);
                            } catch (IOException e) {
                                logger.warning("could not put item to new KVServer: " + item);
                            }
                            return null;
                        }).collect(Collectors.toSet())
                );

                futures.forEach((f) -> {
                    try {
                        f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        logger.warning("failed to finish putting value to new predecessor: " + e.getMessage());
                    }
                });

                sendFinish(finalEcsClient);
            } catch (InterruptedException e) {
                logger.warning("interrupted while putting values to new predecessor");
            }
        });
    }

    private void sendFinish(KVTP2Client ecsClient) {
        Message finish = new Message("finish");
        finish.put("ecsip", kvServer.getControlAPIServerAddress().getHostString());
        finish.put("ecsport", Integer.toString(kvServer.getControlAPIServerAddress().getPort()));
        try {
            Message res = ecsClient.send(finish);
            if (res.getCommand().equals("bye")) {
                kvServer.setStopped(true);
            }
            ecsClient.close();
        } catch (IOException e) {
            logger.warning("failed to send finish for shutdown to ecs: " + e.getMessage());
        }
    }
}
