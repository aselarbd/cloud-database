package de.tum.i13.kvtp2.middleware;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;

import java.util.function.BiConsumer;

/**
 * Handlers can be wrapped to provide middleware functionalities.
 * This is implemented by using the chain-of-responsibility-pattern.
 */
@FunctionalInterface
public interface Handler {

    /**
     * handle handles an incoming Message. The provided MessageWriter
     * can be used to send responses back to the client.
     *
     * @param writer A writer to which any response can be sent.
     * @param message The received message to be handled.
     */
    void handle(MessageWriter writer, Message message);

    /**
     * To use this handler as a middleware handler, just call next on setup
     * and provide the next handler in chain, to which the request will be
     * handed after this handler finished. Next can be overwritten to do more
     * extended handling, like decisions, whether the next handler should be
     * executed at all, which is useful for breaking the chain, if e.g. a response
     * is invalid.
     *
     * @param next the next handler in the chain
     * @return a handler, which executes this handler and than hands over the request to the given next handler.
     */
    default Handler next(Handler next) {
        return (w, m) -> {
            handle(w, m);
            next.handle(w, m);
        };
    }
}
