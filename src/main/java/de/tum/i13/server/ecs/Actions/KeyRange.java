package de.tum.i13.server.ecs.Actions;

import de.tum.i13.shared.ECSMessage;

public class KeyRange extends Action {

    @Override
    public ECSMessage.MsgType execute() {
        keyRange.put(receiver);

        ECSMessage keyRangeMessage = new ECSMessage(ECSMessage.MsgType.KEYRANGE);
        keyRangeMessage.addKeyrange(0, keyRange);
        String ecsMessage = keyRangeMessage.getFullMessage();
        sender.sendTo(receiver, ecsMessage);
        return ECSMessage.MsgType.RESPONSE_OK;
    }
}
