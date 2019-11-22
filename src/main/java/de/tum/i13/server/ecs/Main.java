package de.tum.i13.server.ecs;

import de.tum.i13.kvtp.Server;
import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class Main {

    public static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);
        setupLogging(cfg.logfile, cfg.loglevel);

        logger.info("Starting ECS Server");
        logger.info("Config: " + cfg.toString());

        Server s = new Server();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing ecs server");
            s.close();
        }));

        CommandProcessor ecs = new ECSCommandProcessor(s);

        s.bindSockets(cfg.listenaddr, cfg.port, ecs);

        s.start();
    }
}
