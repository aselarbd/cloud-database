package de.tum.i13.server.ecs;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.kvtp.Server;
import de.tum.i13.shared.ECSMessage;
import de.tum.i13.shared.HeartbeatSender;
import de.tum.i13.shared.parsers.ECSMessageParser;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class ECSCommandProcessor implements CommandProcessor {

    private static final boolean DEBUG = false;
    private static Logger logger = Logger.getLogger(ECSCommandProcessor.class.getName());

    private Server sender;

    private ServerStateMap ssm;

    public ECSCommandProcessor(Server sender) {
        this.sender = sender;
        this.ssm = new ServerStateMap();
    }

    @Override
    public String process(InetSocketAddress src, String command) {

        logger.info("got ecs command: " + command);
        ECSMessageParser parser = new ECSMessageParser();
        ECSMessage msg = parser.parse(command);

        switch(msg.getType()) {
            case REGISTER_SERVER:
                register(src, msg);
                return null;

            case ANNOUNCE_SHUTDOWN:
                if (ssm.getByECSAddress(src) == null) {
                    logger.info("dropping command from unknown kvServer: " + msg.getFullMessage());
                    return null;
                }
                handleShutdown(src);
                return null;

            case RESPONSE_OK:
                if (ssm.getByECSAddress(src) == null) {
                    logger.info("dropping command from unknown kvServer: " + msg.getFullMessage());
                    return null;
                }
                handleOK(src);
                return null;

            case RESPONSE_ERROR:
            default:
                if (ssm.getByECSAddress(src) == null) {
                    logger.info("dropping command from unknown kvServer: " + msg.getFullMessage());
                    return null;
                }
                return new ECSMessage(ECSMessage.MsgType.RESPONSE_ERROR).getFullMessage();
        }
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("new connection: " + remoteAddress.toString());
        return "ECSCommandProcessor connected: " + address + " to " + remoteAddress;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }

    private void register(InetSocketAddress ecsAddr, ECSMessage msg) {
        InetSocketAddress kvAddr = msg.getIpPort(0);

        ServerState serverState = new ServerState(ecsAddr, kvAddr);
        ssm.add(serverState);
        heartbeat(serverState);

        ECSMessage keyRangeMessage = new ECSMessage(ECSMessage.MsgType.KEYRANGE);
        keyRangeMessage.addKeyrange(0, ssm.getKeyRanges());
        String keyRangeString = keyRangeMessage.getFullMessage();

        ServerState predecessor = ssm.getKVPredecessor(serverState);
        if (predecessor.getKV() != kvAddr) {
            ECSMessage writeLockMessage = new ECSMessage(ECSMessage.MsgType.WRITE_LOCK);
            sender.sendTo(predecessor.getECS(), writeLockMessage.getFullMessage());
            sender.sendTo(predecessor.getECS(), keyRangeString);
            ssm.setState(predecessor, ServerState.State.BALANCE);
        }

        sender.sendTo(ecsAddr, keyRangeString);
        serverState.setState(ServerState.State.ACTIVE);
    }

    private void heartbeat(ServerState receveiver) {
        HeartbeatSender heartbeatSender = new HeartbeatSender(receveiver.getKV());
        ScheduledExecutorService heartBeatService = heartbeatSender.start(() -> {
            if (!DEBUG) {
                ssm.remove(receveiver);
                broadcastKeyrange();
            }
        });
        receveiver.addShutdownHook(heartBeatService::shutdown);
    }

    private void handleOK(InetSocketAddress src) {
        ServerState server = ssm.getByECSAddress(src);

        if (server.isBalancing()) {
            releaseLock(server);
            server.setState(ServerState.State.ACTIVE);
            broadcastKeyrange();
        }

        if (server.isShuttingDown()) {
            broadcastKeyrange();
            shutdown(server);
        }
    }

    private void shutdown(ServerState server) {
        sender.sendTo(server.getECS(), new ECSMessage(ECSMessage.MsgType.REL_LOCK).getFullMessage());
        ssm.remove(server);
    }

    private void releaseLock(ServerState server) {
        ECSMessage releaseMsg = new ECSMessage(ECSMessage.MsgType.REL_LOCK);
        sender.sendTo(server.getECS(), releaseMsg.getFullMessage());
        server.setState(ServerState.State.ACTIVE);
    }

    private void handleShutdown(InetSocketAddress src) {
        ServerState server = ssm.getByECSAddress(src);
        server.setState(ServerState.State.SHUTDOWN);
        ssm.getKeyRanges().remove(server.getKV());

        sender.sendTo(src, new ECSMessage(ECSMessage.MsgType.WRITE_LOCK).getFullMessage());

        ServerState successor = ssm.getKVSuccessor(server);
        ECSMessage keyRangeMsg = new ECSMessage(ECSMessage.MsgType.KEYRANGE);
        keyRangeMsg.addKeyrange(0, ssm.getKeyRanges());
        sender.sendTo(successor.getECS(), keyRangeMsg.getFullMessage());
        sender.sendTo(src, keyRangeMsg.getFullMessage());
    }

    private void broadcastKeyrange() {
        ECSMessage keyRangeMsg = new ECSMessage(ECSMessage.MsgType.KEYRANGE);
        keyRangeMsg.addKeyrange(0, ssm.getKeyRanges());
        String msg = keyRangeMsg.getFullMessage();
        for (InetSocketAddress isa : ssm.getECSBroadcastSet()) {
            sender.sendTo(isa, msg);
        }
    }
}
