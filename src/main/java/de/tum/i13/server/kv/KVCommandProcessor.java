package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {

    public static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    private final KVStore kvStore;

    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public String process(String command) {
        //TODO
        //TODO: In case you are using nio and want to process non-blocking, it needs more changes of this interface.

        try {
            logger.info("try to parse command");
            String[] parts = command.split("\\s+");
            String key;
            switch (parts[0].trim().toLowerCase()) {
                case "put":
                    logger.info("parsed 'put' command");
                    key = parts[1];
                    String value = command.split(key + " ")[1];
                    synchronized (kvStore) {
                        logger.info("put " + key + ":" + value);
                        this.kvStore.put(key, value);
                    }
                    return "success";
                case "get":
                    logger.info("parsed 'get' command");
                    key = parts[1];
                    String result;
                    synchronized (kvStore) {
                        logger.info("get " + key);
                        result = kvStore.get(key);
                    }
                    return result;
                default:
                    logger.info("unknown command");
                    return "unknown command";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        //TODO

        return null;
    }

    @Override
    public void connectionClosed(InetAddress address) {
        //TODO

    }
}
