package de.tum.i13.server.kv.state.requests;

import de.tum.i13.shared.ConsistentHashMap;

public class ReBalanceRequest extends Request {

    private ConsistentHashMap keyRange;

    public ReBalanceRequest() {
        super(RequestType.REBALANCE);
    }

    public void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRange = keyRange;
    }

    public ConsistentHashMap getKeyRange() {
        return keyRange;
    }
}
