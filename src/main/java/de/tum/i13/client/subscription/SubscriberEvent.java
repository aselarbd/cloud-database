package de.tum.i13.client.subscription;

import java.net.InetSocketAddress;

/**
 * Wrapper for events occurring in a {@link Subscriber}
 */
public class SubscriberEvent {
    public enum EventType {
        SERVER_NOT_RESPONSIBLE,
        SERVER_DOWN,
        SERVER_NOT_READY,
        OTHER
    }

    EventType type;
    String message;
    InetSocketAddress src;

    public SubscriberEvent(InetSocketAddress src, EventType type, String message) {
        this.src = src;
        this.type = type;
        this.message = message;
    }

    public InetSocketAddress getSource() {
        return src;
    }

    public EventType getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }
}
