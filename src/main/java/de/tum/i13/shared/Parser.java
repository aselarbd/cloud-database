package de.tum.i13.shared;

import java.util.Arrays;

/**
 * Basic logic to parse a command, or more generic, a string having the form
 * <br>
 * &lt;name&gt; &lt;arg1&gt; &lt;arg2&gt; ...
 * <br><br>
 * It allows to turn the string into a specific object representation.
 */
public abstract class Parser<T> {
    private final static Log logger = new Log(Parser.class);
    /**
     * Indicates the number of expected arguments
     *
     * @return Number of arguments for this command
     */
    protected abstract int getArgCount();

    /**
     * Indicates if the command has a variable number of arguments.
     * In this case, {@link #getArgCount()} provides the minimum number of expected arguments.
     *
     * @return True if a variable number of arguments is expected for this command.
     */
    protected abstract boolean hasVariableArgs();

    /**
     * Parse the splitted arguments, gets called as part of {@link #parse(String)}
     *
     * @param name The command name (first item)
     * @param args Input splitted by spaces
     * @return A new object based on the given arguments, or null if the arguments are invalid.
     */
    protected abstract T parseArgs(String name, String[] args);

    public boolean requiresArguments() {
        return (getArgCount() > 0);
    }

    /**
     * Parses the given input string to be interpreted as this command.
     *
     * @param input String to be parsed
     * @return A new object based on the given arguments, or null if the input is invalid.
     */
    public T parse(String input) {
        if (input == null || input.isEmpty()) {
            return null;
        }
        String[] command = input.split(" ");
        if (hasVariableArgs() && command.length - 1 < getArgCount()) {
            logger.fine("Too few arguments for variable-length command");
            return null;
        } else if (!hasVariableArgs() && command.length - 1 != getArgCount()) {
            logger.fine("Too few arguments for command");
            return null;
        }
        // basic checks passed, now perform command-specific checks.
        return parseArgs(command[0], Arrays.copyOfRange(command, 1, command.length));
    }
}
