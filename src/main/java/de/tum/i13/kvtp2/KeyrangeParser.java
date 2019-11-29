package de.tum.i13.kvtp2;

public class KeyrangeParser extends Parser {

    int position = 0;

    @Override
    public Parser with(String argument) {
        if (position == 0) {
            this.command = argument;
            position++;
            return this;
        }
        pairs.put(firstArgName, argument);
        return this;
    }

}
