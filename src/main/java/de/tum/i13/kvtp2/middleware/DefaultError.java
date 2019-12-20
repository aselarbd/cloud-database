package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;

public class DefaultError implements Handler {

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        Message response = Message.getResponse(message);
        response.setCommand("error");
        response.put("msg", "no handler found for Request command: " + message.getCommand());
        messageWriter.write(response);
        messageWriter.flush();
    }
}
