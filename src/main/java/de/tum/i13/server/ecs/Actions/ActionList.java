package de.tum.i13.server.ecs.Actions;

import de.tum.i13.shared.ECSMessage;

import java.util.ArrayList;
import java.util.List;

public class ActionList {

    private ECSMessage.MsgType expected;

    private List<Action> actions;

    public ActionList() {
        actions = new ArrayList<>();
    }

    public ECSMessage.MsgType getExpected() {
        return this.expected;
    }

    public void add(Action a) {
        this.actions.add(a);
    }

    public void step() {
        Action next = actions.get(0);
        this.expected = next.getExpectedNextMessage();
        actions.remove(0);
        next.execute();
    }

    public void clear() {
        actions.clear();
    }
}
