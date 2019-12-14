package de.tum.i13.server.ecs;

import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class ECSMain {

    public static final Logger logger = Logger.getLogger(ECSMain.class.getName());
    private Server server;

    public ECSMain(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);
        setupLogging(cfg.logfile, cfg.loglevel);

        logger.info("Starting ECS Server");
        logger.info("Config: " + cfg.toString());

        server = new Server(cfg.listenaddr, cfg.port);
    }

    public void run() throws IOException {
        server.start();
    }

    public void shutdown() {
        logger.info("Closing ecs server");
        try {
            server.close();
        } catch (IOException e) {
            logger.warning("Exception on shutdown: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        ECSMain main = new ECSMain(args);
        Runtime.getRuntime().addShutdownHook(new Thread(main::shutdown));
        main.run();
    }
}
