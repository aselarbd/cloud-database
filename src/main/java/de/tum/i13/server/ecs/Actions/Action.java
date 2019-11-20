package de.tum.i13.server.ecs.Actions;

import de.tum.i13.kvtp.Server;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.ECSMessage;

import java.net.InetSocketAddress;
import java.util.List;

public abstract class Action {

    protected ConsistentHashMap keyRange;
    Server sender;
    InetSocketAddress receiver;

    private ECSMessage.MsgType expectedNextMessage;
    private InetSocketAddress server;

    public ECSMessage.MsgType getExpectedNextMessage() {
        return expectedNextMessage;
    }

    public Action withExpectedNextMessage(ECSMessage.MsgType expectedNextMessage) {
        this.expectedNextMessage = expectedNextMessage;
        return this;
    }

    public Action withSender(Server sender) {
        this.sender = sender;
        return this;
    }

    public Action withReceiver(InetSocketAddress receiver) {
        this.receiver = receiver;
        return this;
    }

    public Action withKeyRange(ConsistentHashMap keyRange) {
        this.keyRange = keyRange;
        return this;
    }

    public Action withNewServer(InetSocketAddress server) {
        this.server = server;
        return this;
    }

    public abstract ECSMessage.MsgType execute();
}
