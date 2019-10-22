package de.tum.i13.server.nio;

import de.tum.i13.server.echo.EchoLogic;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartNioServer {

    public static Logger logger = Logger.getLogger(StartNioServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile, cfg.loglevel);
        logger.info("Config: " + cfg.toString());

        logger.info("starting server");

        //Replace with your Key Value command processor
        CommandProcessor echoLogic = new EchoLogic();

        NioServer sn = new NioServer(echoLogic);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing NioServer");
            sn.close();
        }));

        sn.bindSockets(cfg.listenaddr, cfg.port);
        System.out.println("KV Server started");
        sn.start();
    }
}
