package de.tum.i13.server.ecs.Actions;

import de.tum.i13.shared.ECSMessage;

public class NextAddr extends Action {


    @Override
    public ECSMessage.MsgType execute() {
        ECSMessage ecsMessage = new ECSMessage(ECSMessage.MsgType.NEXT_ADDR);
//        ecsMessage.addIpPort(0, nextIP);
        sender.sendTo(receiver, ecsMessage.getFullMessage());
        return ECSMessage.MsgType.RESPONSE_OK;
    }
}
