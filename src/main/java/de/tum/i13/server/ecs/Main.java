package de.tum.i13.server.ecs;

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

        logger.info("Config: " + cfg.toString());

        Server s = new Server();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing ecs server");
            try {
                s.close();
            } catch (IOException e) {
                logger.severe("Could not close server, shutting down");
            }
        }));

        s.init(cfg.listenaddr, cfg.port);
        s.start();
    }
}
