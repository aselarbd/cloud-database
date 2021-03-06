package de.tum.i13.shared.parsers;

import de.tum.i13.shared.Parser;
import de.tum.i13.shared.KVItem;

import java.util.Arrays;

/**
 * Parses a command/string dealing with a key-value pair. This pair is represented as {@link KVItem}.
 */
public class KVItemParser extends Parser<KVItem> {
    private final boolean requiresValue;

    /**
     * Creates a new parser responsible for the command with the given name.
     *
     * The command has the structure "&lt;name&gt; key [value]".
     *
     * @param requiresValue True if the command requires key and value arguments.
     *                      Otherwise, only a key is expected.
     */
    public KVItemParser(boolean requiresValue) {
        this.requiresValue = requiresValue;
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
    protected KVItem parseArgs(String name, String[] args) {
        return KVItemParser.itemFromArgs(args);
    }

    /**
     * Creates a KVItem from a string argument array.
     *
     * @param args Space-separated arguments, e.g. produced by a parser
     * @return a new KVItem with the first item interpreted as key
     */
    public static KVItem itemFromArgs(String[] args) {
        if (args.length == 0) {
            return null;
        } else if (args.length == 1) {
            return new KVItem(args[0]);
        } else {
            String[] value = Arrays.copyOfRange(args, 1, args.length);
            return new KVItem(args[0], String.join(" ", value));
        }
    }
}
