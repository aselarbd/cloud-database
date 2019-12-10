package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.ServerState;
import de.tum.i13.server.ecs.ServerStateMap;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

public class AddressConverter implements BiConsumer<MessageWriter, Message> {

    private final ServerStateMap ssm;

    public AddressConverter(ServerStateMap ssm) {
        this.ssm = ssm;
    }

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        String command = message.getCommand();
        if (command.equals("ecs_to_kv")) {
            String ecsIP = message.get("ecsip");
            String ecsPort = message.get("ecsport");
            Message response = Message.getResponse(message);
            ServerState kvAddress = ssm.getByECSAddress(new InetSocketAddress(ecsIP, Integer.parseInt(ecsPort)));
            response.put("kvip", kvAddress.getKV().getHostString());
            response.put("kvport", Integer.toString(kvAddress.getKV().getPort()));
            messageWriter.write(response);
            return;
        } else if (command.equals("kv_to_ecs")) {
            String kvIP = message.get("kvip");
            String kvPort = message.get("kvport");
            Message response = Message.getResponse(message);
            ServerState ecsAddress = ssm.getByKVAddress(new InetSocketAddress(kvIP, Integer.parseInt(kvPort)));
            response.put("ecsip", ecsAddress.getECS().getHostString());
            response.put("ecsport", Integer.toString(ecsAddress.getECS().getPort()));
            messageWriter.write(response);
            return;
        }

        Message response = Message.getResponse(message);
        response.setCommand("error");
        response.put("msg", "unknown command");
        messageWriter.write(response);
    }
}
