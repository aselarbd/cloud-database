package de.tum.i13.client;

import de.tum.i13.shared.CommandParser;

import java.util.function.Consumer;

/**
 * Connects a parser with an action to be executed if parsing is successful.
 *
 * @param <T>
 */
public class Action<T> {
    private CommandParser<T> parser;
    private Consumer<T> action;

    public Action(CommandParser<T> parser, Consumer<T> action) {
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
