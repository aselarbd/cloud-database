package de.tum.i13.server.ecs;

import de.tum.i13.kvtp.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class ECSCommandProcessor implements CommandProcessor {

    private static Logger logger = Logger.getLogger(ECSCommandProcessor.class.getName());

    @Override
    public String process(String command) {
        String[] cmdArgs = command.split("\\s+");

        switch(cmdArgs[0]) {
            case "ok":
            case "error":
                logger.info("got answer: " + command);
                return null;
            case "register":
            case "announce_shutdown":
            case "done":
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
        logger.info("new connection: " + remoteAddress.toString());
        return "connected to ECS-Server: " + address.toString();
    }

    @Override
    public void connectionClosed(InetAddress address) {
        logger.info("connection closed: " + address.toString());
    }
}
