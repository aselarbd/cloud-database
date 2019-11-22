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
            case RESPONSE_ERROR:

            case RESPONSE_OK:

            case REGISTER_SERVER:
                register(src, msg);
                return null;

            case ANNOUNCE_SHUTDOWN:

            case PUT_DONE:

            default:
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
        sender.sendTo(ecsAddr, keyRangeString);

        ServerState predecessor = ssm.getKVPredecessor(serverState);

        if (predecessor.getKV() != kvAddr) {
            ECSMessage writeLockMessage = new ECSMessage(ECSMessage.MsgType.WRITE_LOCK);
            sender.sendTo(predecessor.getECS(), writeLockMessage.getFullMessage());
            sender.sendTo(predecessor.getECS(), keyRangeString);
            ssm.setState(predecessor, ServerState.State.BALANCE);
        }
    }

    private void heartbeat(ServerState receveiver) {
        HeartbeatSender heartbeatSender = new HeartbeatSender(receveiver.getKV());
        ScheduledExecutorService heartBeatService = heartbeatSender.start(() -> {
            ssm.remove(receveiver);
            braodcastKeyrange();
        });
        receveiver.addShutdownHook(() -> {
            heartBeatService.shutdown();
        });
    }

    private void braodcastKeyrange() {
        ECSMessage keyRangeMsg = new ECSMessage(ECSMessage.MsgType.KEYRANGE);
        keyRangeMsg.addKeyrange(0, ssm.getKeyRanges());
        String msg = keyRangeMsg.getFullMessage();
        for (InetSocketAddress isa : ssm.getECSBroadcastSet()) {
            sender.sendTo(isa, msg);
        }
    }
}
