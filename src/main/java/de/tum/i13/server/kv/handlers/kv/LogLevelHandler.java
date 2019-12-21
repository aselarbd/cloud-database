package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.shared.LogLevelChange;
import de.tum.i13.shared.LogSetup;

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogLevelHandler implements BiConsumer<MessageWriter, Message> {

    private static final Logger LOGGER = Logger.getLogger(LogLevelHandler.class.getName());

    @Override
    public void accept(MessageWriter messageWriter, Message message) {
        String level = message.get("level");
        Level newLevel = Level.parse(level);
        LogLevelChange change = LogSetup.changeLoglevel(newLevel);

        String msg = "Changed log Level from " + change.getPreviousLevel().toString() + " to "
                + change.getNewLevel().toString();
        LOGGER.info(msg);

        Message logLevelResponse = Message.getResponse(message);
        logLevelResponse.setCommand("serverLogLevel");
        logLevelResponse.put("serverLogLevel",msg);
        messageWriter.write(logLevelResponse);

    }
}
