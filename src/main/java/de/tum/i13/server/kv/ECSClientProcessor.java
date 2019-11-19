package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

// TODO: Not happy with this whole idea of using a CommandProcessor
//  implementation as ECSClient
public class ECSClientProcessor implements CommandProcessor {

    public static Logger logger = Logger.getLogger(ECSClientProcessor.class.getName());

    @Override
    public String process(String command) {
        String[] cmdArgs = command.split("\\s+");

        switch(cmdArgs[0]) {
            case "ok":
            case "error":
            case "connected": // TODO: maybe handle this confirmation message after init connection differently
                logger.info("got answer: " + command);
                return null;
            case "write_lock":
            case "release_lock":
            case "next_addr":
            case "transfer_range":
            case "broadcast_new":
            case "broadcast_rem":
            case "keyrange":
            case "ping": // TODO: move to separate processor to handle via udp?
                logger.info("got: " + cmdArgs[0]);
                break;
            default:
                logger.info("got unknown cmd: " + command);
                return "error";
        }
        return "ok";
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        logger.info("connected to ECS Service, registering new node");
        return "register";
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }
}
