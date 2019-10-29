package de.tum.i13.shared.commandparsers;

import de.tum.i13.shared.CommandParser;
import de.tum.i13.shared.KVItem;

import java.util.Arrays;

/**
 * Parses a command dealing with a key-value pair. This pair is represented as {@link KVItem}.
 */
public class KVItemCommandParser extends CommandParser<KVItem> {
    private String name;
    private boolean requiresValue;

    /**
     * Creates a new parser responsible for the command with the given name.
     *
     * The command has the structure "&lt;name&gt; key [value]".
     *
     * @param name Command to be parsed by this instance.
     * @param requiresValue True if the command requires key and value arguments.
     *                      Otherwise, only a key is expected.
     */
    public KVItemCommandParser(String name, boolean requiresValue) {
        this.name = name;
        this.requiresValue = requiresValue;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected int getArgCount() {
        return requiresValue ? 2 : 1;
    }

    @Override
    protected boolean hasVariableArgs() {
        // values may contain spaces
        return requiresValue;
    }

    @Override
    protected KVItem parseArgs(String[] args) {
        if (requiresValue) {
            String[] value = Arrays.copyOfRange(args, 1, args.length);
            return new KVItem(args[0], String.join(" ", value));
        } else {
            return new KVItem(args[0]);
        }
    }
}
