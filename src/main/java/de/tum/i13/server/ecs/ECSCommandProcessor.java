package de.tum.i13.server.ecs;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.kvtp.Server;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.ECSMessage;
import de.tum.i13.shared.parsers.ECSMessageParser;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ECSCommandProcessor implements CommandProcessor {

    private static Logger logger = Logger.getLogger(ECSCommandProcessor.class.getName());

    private Server sender;

    private ServerStateMap ssm;

    public ECSCommandProcessor(Server sender) throws NoSuchAlgorithmException {
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
}
