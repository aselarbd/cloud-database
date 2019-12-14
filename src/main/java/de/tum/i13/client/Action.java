package de.tum.i13.client;

import de.tum.i13.shared.Parser;

import java.util.function.Consumer;

/**
 * Connects a parser with an action to be executed if parsing is successful.
 *
 * @param <T>
 */
public class Action<T> {
    private final Parser<T> parser;
    private final Consumer<T> action;

    public Action(Parser<T> parser, Consumer<T> action) {
        this.parser = parser;
        this.action = action;
    }

    public boolean run(String input) {
        T result = parser.parse(input);
        if (result != null) {
            action.accept(result);
            return true;
        }
        return false;
    }
}
