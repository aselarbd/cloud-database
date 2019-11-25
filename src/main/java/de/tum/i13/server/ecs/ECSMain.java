package de.tum.i13.server.ecs;

import de.tum.i13.kvtp.Server;
import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class ECSMain {

    public static Logger logger = Logger.getLogger(ECSMain.class.getName());
    private Server server;

    public ECSMain(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);
        setupLogging(cfg.logfile, cfg.loglevel);

        logger.info("Starting ECS Server");
        logger.info("Config: " + cfg.toString());

        server = new Server();
        CommandProcessor ecs = new ECSCommandProcessor(server);

        server.bindSockets(cfg.listenaddr, cfg.port, ecs);
    }

    public void run() throws IOException {
        server.start();
    }

    public void shutdown() {
        logger.info("Closing ecs server");
        server.close();
    }

    public static void main(String[] args) throws IOException {
        ECSMain main = new ECSMain(args);
        Runtime.getRuntime().addShutdownHook(new Thread(main::shutdown));
        main.run();
    }
}
