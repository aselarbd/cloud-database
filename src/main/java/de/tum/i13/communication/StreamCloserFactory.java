package de.tum.i13.communication;

/**
 * Interface for providers of StreamClosers
 */
public interface StreamCloserFactory {

    /**
     * Creates a new StreamCloser.
     *
     * @return an object that implements the StreamCloser interface.
     */
    StreamCloser createStreamCloser();
}
