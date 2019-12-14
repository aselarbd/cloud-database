package de.tum.i13.shared.parsers;

import de.tum.i13.shared.Parser;

/**
 * Very simple generic parser which can handle basic commands/strings in a flexible way.
 * The data is represented as String array.
 */
public class StringArrayParser extends Parser<String[]> {
    private final int argCount;
    private final boolean variableArgs;

    /**
     * Creates a new parser responsible for the command with the given name.
     *
     * @param argCount Required number of arguments
     * @param variableArgs true if the <code>argCount</code> is a minimum number of arguments.
     */
    public StringArrayParser(int argCount, boolean variableArgs) {
        this.argCount = argCount;
        this.variableArgs = variableArgs;
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
    protected String[] parseArgs(String name, String[] args) {
        return args;
    }
}
