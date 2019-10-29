package de.tum.i13.shared;

import java.util.Arrays;

/**
 * Basic logic to parse a command. It performs checks like valid input, encoding etc.
 */
public abstract class CommandParser<T> {
    /**
     * The command's name, i.e. the string which a command is invoked with.
     *
     * @return The command name
     */
    public abstract String getName();

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
     * @param args Input splitted by spaces
     * @return A new object based on the given arguments, or null if the arguments are invalid.
     */
    protected abstract T parseArgs(String[] args);

    /**
     * Parses the given input string to be interpreted as this command.
     *
     * @param input String to be parsed
     * @return A new object based on the given arguments, or null if the input is invalid.
     */
    public T parse(String input) {
        String[] command = input.split(" ");
        if (!checkEncoding(input)) {
            return null;
        }
        if (!command[0].equals(getName())) {
            return null;
        }
        if (hasVariableArgs() && command.length - 1 < getArgCount()) {
            return null;
        } else if (!hasVariableArgs() && command.length - 1 != getArgCount()) {
            return null;
        }
        // basic checks passed, now perform command-specific checks
        return parseArgs(Arrays.copyOfRange(command, 1, command.length));
    }

    /**
     * Ensures the input has the specified encoding and doesn't contain newline
     * characters.
     *
     * @param input String to check
     * @return True if the input is valid in terms of encoding
     */
    protected boolean checkEncoding(String input) {
        // TODO
        return true;
    }
}
