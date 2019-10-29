package de.tum.i13.communication.impl;

import de.tum.i13.communication.StreamCloser;
import de.tum.i13.communication.StreamCloserFactory;

/**
 * Provides a StreamCloser using the SocketStreamCloser
 */
public class SocketStreamCloserFactory implements StreamCloserFactory {
    @Override
    public StreamCloser createStreamCloser() {
        return new SocketStreamCloser();
    }
}
