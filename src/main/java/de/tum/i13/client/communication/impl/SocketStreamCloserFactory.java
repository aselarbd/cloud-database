package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.StreamCloser;
import de.tum.i13.client.communication.StreamCloserFactory;

/**
 * Provides a StreamCloser using the SocketStreamCloser
 */
public class SocketStreamCloserFactory implements StreamCloserFactory {
    @Override
    public StreamCloser createStreamCloser() {
        return new SocketStreamCloser();
    }
}
