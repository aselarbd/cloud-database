package de.tum.i13.server.ecs.Actions;

import de.tum.i13.shared.ECSMessage;

public class BroadcastNew extends Action {
    @Override
    public ECSMessage.MsgType execute() {
        return ECSMessage.MsgType.RESPONSE_ERROR;
    }
}
