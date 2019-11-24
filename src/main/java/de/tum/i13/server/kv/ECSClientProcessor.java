package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.kvtp.Server;
import de.tum.i13.shared.*;
import de.tum.i13.shared.parsers.ECSMessageParser;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

// TODO: Not happy with this whole idea of using a CommandProcessor
//  implementation as ECSClient
public class ECSClientProcessor implements CommandProcessor {

    public static Logger logger = Logger.getLogger(ECSClientProcessor.class.getName());

    private Server sender;
    private KVCommandProcessor kvCommandProcessor;
    private InetSocketAddress ecsAddr;
    private ScheduledExecutorService heartBeatService;

    public ECSClientProcessor(Server sender, InetSocketAddress ecsAddr, KVCommandProcessor kvCommandProcessor) {
        this.sender = sender;
        this.ecsAddr = ecsAddr;
        this.kvCommandProcessor = kvCommandProcessor;
    }

    // TODO: Call this where appropriate
    public void shutdown() {
        heartBeatService.shutdown();
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

        logger.info("got ecs command: " + command);
        ECSMessageParser parser = new ECSMessageParser();
        ECSMessage msg = parser.parse(command);

        if (msg == null) {
            return null;
        }

        switch(msg.getType()) {
            case RESPONSE_OK:
            case RESPONSE_ERROR:
            case WRITE_LOCK:
                kvCommandProcessor.setWriteLock();
                return null;
            case REL_LOCK:
                kvCommandProcessor.releaseWriteLock();
                return null;
            case NEXT_ADDR:
            case TRANSFER_RANGE:
            case BROADCAST_NEW:
            case BROADCAST_REM:
                break;
            case KEYRANGE:
                ConsistentHashMap newKeyRange = msg.getKeyrange(0);

                InetSocketAddress newPredecessor = newKeyRange.getPredecessor(kvCommandProcessor.getAddr());

                // this checks, whether the new predecessor is part of the old keyrange.
                // If so, we have to give him all the data up to his position.
                if (kvCommandProcessor.getAddr().equals(kvCommandProcessor.getKeyRange().get(newPredecessor))) {

                    ECSClientProcessor ecsClientProcessor = this;
                    Set<String> handOffKeys = kvCommandProcessor.getAllKeys((s) -> !newKeyRange.get(s).equals(kvCommandProcessor.getAddr()));
                    handOffKeys.forEach((s) -> {
                        try {
                            sender.connectTo(newKeyRange.get(s), ecsClientProcessor);
                            KVItem item = kvCommandProcessor.getItem(s);
                            if (!item.getValue().equals(Constants.DELETE_MARKER)) {
                                sender.sendTo(newKeyRange.get(s), "put " + item.getKey() + " " + item.getValue());
                            }
                            kvCommandProcessor.delete(new KVItem(s));
                        } catch (IOException e) {
                            logger.warning("Failed to put off key value pair for key: " + s + " continue without deleting");
                        }
                    });
                }

                // no just set the new keyrange, new keys (if any) will come soon.
                kvCommandProcessor.setKeyRange(newKeyRange);
                sender.sendTo(ecsAddr, "done"); // tell that you're done
                return null;
        }
        return null;
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
