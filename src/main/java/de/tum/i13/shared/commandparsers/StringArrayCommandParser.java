package de.tum.i13.shared.commandparsers;

import de.tum.i13.shared.CommandParser;

/**
 * Very simple generic parser which can handle basic commands in a flexible way.
 * The data is represented as String array.
 */
public class StringArrayCommandParser extends CommandParser<String[]> {
    private String name;
    private int argCount;
    private boolean variableArgs;

    /**
     * Creates a new parser responsible for the command with the given name.
     *
     * @param name Command to be parsed by this instance.
     * @param argCount Required number of arguments
     * @param variableArgs true if the <code>argCount</code> is a minimum number of arguments.
     */
    public StringArrayCommandParser(String name, int argCount, boolean variableArgs) {
        this.name = name;
        this.argCount = argCount;
        this.variableArgs = variableArgs;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    protected int getArgCount() {
        return this.argCount;
    }

    @Override
    protected boolean hasVariableArgs() {
        return this.variableArgs;
    }

    @Override
    protected String[] parseArgs(String[] args) {
        return args;
    }
}
