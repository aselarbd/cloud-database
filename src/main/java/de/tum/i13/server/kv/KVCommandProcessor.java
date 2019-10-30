package de.tum.i13.server.kv;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.parsers.KVResultParser;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class KVCommandProcessor implements CommandProcessor {

    private static Logger logger = Logger.getLogger(KVCommandProcessor.class.getName());

    private final KVStore kvStore;

    public KVCommandProcessor(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public String process(String input) {
        KVResultParser parser = new KVResultParser();
        KVResult command = parser.parse(input);

        switch(command.getMessage().toLowerCase()) {
            case "get":
                String key = command.getItem().getKey();
                KVItem result = kvStore.get(key);
                if (result != null) {
                    return result.getValue();
                }
                return "get_error " + key;
            case "put":
                kvStore.put(command.getItem());
                return "put_success";
            case "delete":
                kvStore.delete(command.getItem());
                break;
            default:
                return "unknown command";
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
