package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.shared.Log;
import de.tum.i13.shared.LogLevelChange;
import de.tum.i13.shared.LogSetup;

import java.util.logging.Level;

public class LogLevelHandler implements Handler {

    private static final Log logger = new Log(LogLevelHandler.class);

    @Override
    public void handle(MessageWriter messageWriter, Message message) {
        String level = message.get("level");
        Level newLevel = Level.parse(level);
        LogLevelChange change = LogSetup.changeLoglevel(newLevel);

        String msg = "Changed log Level from " + change.getPreviousLevel().toString() + " to "
                + change.getNewLevel().toString();
        logger.info(msg);

        Message logLevelResponse = Message.getResponse(message);
        logLevelResponse.setCommand("serverLogLevel");
        logLevelResponse.put("serverLogLevel",msg);
        messageWriter.write(logLevelResponse);

    }
}
