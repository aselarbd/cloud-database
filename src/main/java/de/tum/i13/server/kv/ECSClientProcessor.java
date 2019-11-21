package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.kvtp.Server;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.ECSMessage;
import de.tum.i13.shared.HeartbeatListener;
import de.tum.i13.shared.parsers.ECSMessageParser;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
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
                try {
                    ConsistentHashMap newKeyRange = msg.getKeyrange(0);

                    InetSocketAddress previousPredecessor = kvCommandProcessor.getKeyRange().getPredecessor(kvCommandProcessor.getAddr());

                    // this checks, whether the previousPredecessor (it's position
                    // on the ConsistentHash-ring) is now part of our key range.
                    // If yes, that means, we have to hand of the data between our previous
                    // predecessor and our new predecessor to some other server(s).
                    // If no, our keyrange grew larger, which just means, that another server
                    // is soon going to start putting new items to this server.
                    if (newKeyRange.get(previousPredecessor).equals(kvCommandProcessor.getAddr())) {
                        // handoff keys
                    }
                    // just set the new keyrange, new keys will come soon.
                    kvCommandProcessor.setKeyRange(newKeyRange);
                    sender.sendTo(ecsAddr, "done"); // tell that you're done
                } catch (NoSuchAlgorithmException e) {
                    logger.severe("Could not create Consistent Hash Map");
                    // TODO: Maybe get rid of these stupid NoSuchAlgorithmExceptions, we can't do anything about it anyway.
                }
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
