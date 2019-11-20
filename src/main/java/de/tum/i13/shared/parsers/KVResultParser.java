package de.tum.i13.shared.parsers;

import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.Parser;

public class KVResultParser extends Parser<KVResult> {
    @Override
    protected int getArgCount() {
        return 0;
    }

    @Override
    protected boolean hasVariableArgs() {
        return true;
    }

    @Override
    protected KVResult parseArgs(String name, String[] args) {
        return new KVResult(name, KVItemParser.itemFromArgs(args));
    }
}
