package de.tum.i13.client.communication;

/**
 * Exception type used for all problems occuring in a SocketCommunicator.
 */
public class SocketCommunicatorException extends Exception {
    public SocketCommunicatorException() { }

    public SocketCommunicatorException(String cause) {
        super(cause);
    }

    public SocketCommunicatorException(Throwable e) {
        super(e);
    }
}
