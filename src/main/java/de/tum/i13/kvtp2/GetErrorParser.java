package de.tum.i13.kvtp2;

public class GetErrorParser extends Parser {

    int position = 0;

    @Override
    public Parser with(String argument) {
        if (position == 0) {
            this.command = argument;
            position++;
            return this;
        } else if(position == 1) {
            this.pairs.put(firstArgName, argument);
            position++;
            return this;
        }
        this.pairs.merge(secondArgName, argument, (a, b) -> a + " " + b);
        return this;
    }
}
