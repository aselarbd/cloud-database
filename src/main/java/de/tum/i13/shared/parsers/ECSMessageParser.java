package de.tum.i13.shared.parsers;

import de.tum.i13.shared.ECSMessage;
import de.tum.i13.shared.Parser;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ECSMessageParser extends Parser<ECSMessage> {
    private final static Logger LOGGER = Logger.getLogger(ECSMessageParser.class.getName());

    @Override
    protected int getArgCount() {
        // use arg count 0 as some elements do not have arguments
        return 0;
    }

    @Override
    protected boolean hasVariableArgs() {
        return true;
    }

    @Override
    protected ECSMessage parseArgs(String name, String[] args) {
        try {
            ECSMessage.MsgType type = null;
            for (ECSMessage.MsgType t : ECSMessage.MsgType.values()) {
                if (t.getMsgName().equals(name)) {
                    type = t;
                    break;
                }
            }
            if (type != null) {
                return new ECSMessage(type, args);
            } else {
                LOGGER.info("Unknown message type: " + name);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.INFO, "Unable to parse ECS message", e);
        }
        // if something went wrong, just return null
        return null;
    }
}
