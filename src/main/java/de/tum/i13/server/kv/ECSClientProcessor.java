package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.kvtp.Server;
import de.tum.i13.shared.*;
import de.tum.i13.shared.parsers.ECSMessageParser;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.logging.Logger;

// TODO: Not happy with this whole idea of using a CommandProcessor
//  implementation as ECSClient
public class ECSClientProcessor implements CommandProcessor {

    public static Logger logger = Logger.getLogger(ECSClientProcessor.class.getName());

    private Server sender;
    private KVCommandProcessor kvCommandProcessor;
    private InetSocketAddress ecsAddr;
    private ScheduledExecutorService heartBeatService;

    private Collection<String> deleteMarkers = new ArrayList<>();

    private boolean shuttingDown = false;
    private boolean shutdownComplete = false;
    private Runnable shutdownHook;

    public ECSClientProcessor(Server sender, InetSocketAddress ecsAddr, KVCommandProcessor kvCommandProcessor) {
        this.sender = sender;
        this.ecsAddr = ecsAddr;
        this.kvCommandProcessor = kvCommandProcessor;
    }

    public Future shutdown(Runnable shutdownHook) {
        return Executors.newSingleThreadScheduledExecutor().submit(() -> {
            shuttingDown = true;
            this.shutdownHook = shutdownHook;
            ECSMessage shutdownMessage = new ECSMessage(ECSMessage.MsgType.ANNOUNCE_SHUTDOWN);
            shutdownMessage.addIpPort(0, kvCommandProcessor.getAddr());
            sender.sendTo(ecsAddr, shutdownMessage.getFullMessage());
        });
    }

    public void register() throws SocketException {
        logger.info("registering new KVServer");

        HeartbeatListener heartbeatListener = new HeartbeatListener();
        this.heartBeatService = heartbeatListener.start(kvCommandProcessor.getAddr().getPort(), kvCommandProcessor.getAddr().getAddress());

        ECSMessage registerMsg = new ECSMessage(ECSMessage.MsgType.REGISTER_SERVER);
        registerMsg.addIpPort(0, kvCommandProcessor.getAddr());

        sender.sendTo(ecsAddr, registerMsg.getFullMessage());
    }

    @Override
    public String process(InetSocketAddress src, String command) {

        logger.info(kvCommandProcessor.getAddr().getHostName() + ":" + kvCommandProcessor.getAddr().getPort() + " got ecs command: " + command);
        ECSMessageParser parser = new ECSMessageParser();
        ECSMessage msg = parser.parse(command);

        if (msg == null) {
            return null;
        }

        switch(msg.getType()) {
            case WRITE_LOCK:
                kvCommandProcessor.setWriteLock();
                return null;
            case REL_LOCK:
                if (shuttingDown) {
                    transfer(kvCommandProcessor.getKeyRange(), kvCommandProcessor.getAllKeys((s) -> true));
                    heartBeatService.shutdown();
                    shutdownHook.run();
                    return null;
                }
                kvCommandProcessor.releaseWriteLock();
                return null;
            case KEYRANGE:
                ConsistentHashMap newKeyRange = msg.getKeyrange(0);

                InetSocketAddress oldPredecessor = kvCommandProcessor.getKeyRange().getPredecessor(kvCommandProcessor.getAddr());
                InetSocketAddress newPredecessor = newKeyRange.getPredecessor(kvCommandProcessor.getAddr());

                if (oldPredecessor != null && !oldPredecessor.equals(newPredecessor)) {
                    Set<String> oldKeys = kvCommandProcessor.getAllKeys((s) ->
                            !newKeyRange
                                    .getSuccessor(s)
                                    .equals(kvCommandProcessor.getAddr())
                    );
                    transfer(newKeyRange, oldKeys);
                    deleteMarkers.addAll(oldKeys);
                } else {
                    deleteMarkedItems();
                }

                // no just set the new keyrange, new keys (if any) will come soon.
                kvCommandProcessor.setKeyRange(newKeyRange);
                sender.sendTo(ecsAddr, new ECSMessage(ECSMessage.MsgType.RESPONSE_OK).getFullMessage()); // tell that you're done
                return null;
            default:
                return null;
        }
    }

    private void deleteMarkedItems() {
        for (String s : deleteMarkers) {
            kvCommandProcessor.delete(new KVItem(s));
        }
        deleteMarkers = new ArrayList<>();
    }

    private void transfer(ConsistentHashMap newKeyRange, Collection<String> keys) {
        ECSClientProcessor ecsClientProcessor = this;

        keys.forEach((s) -> {
            try {
                // TODO: cache connections
                sender.connectTo(newKeyRange.getSuccessor(s), ecsClientProcessor);
                KVItem item = kvCommandProcessor.getItem(s);
                if (item != null && !item.getValue().equals(Constants.DELETE_MARKER)) {
                    sender.sendTo(newKeyRange.getSuccessor(s), "put " + item.getKey() + " " + item.getValue());
                }
            } catch (IOException e) {
                logger.warning("Failed to put off key value pair for key: " + s + " continue without deleting");
            }
        });
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());
        return "ECSClientProcessor connected: " + address + " to " + remoteAddress;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }
}
