package de.tum.i13.kvtp2;

import java.util.LinkedHashMap;
import java.util.Map;

public class Parser {

    protected Message.Type type;
    protected String command;
    protected String firstArgName;
    protected String secondArgName;
    protected Map<String, String> pairs = new LinkedHashMap<>();

    protected boolean closed = false;

    private int position = 0;

    public Parser() {
        this.firstArgName = "key";
        this.secondArgName = "value";
    }

    public Message parse() {
        if (closed) {
            throw new RuntimeException("Parser already used and closed");
        }
        Message message = new Message(type, command);
        pairs.forEach(message::put);
        this.closed = true;
        return message;
    }

    public Parser with(Message.Type type) {
        this.type = type;
        return this;
    }

    public Parser withFirstArgName(String name) {
        this.firstArgName = name;
        return this;
    }

    public Parser withSecondArgName(String name) {
        this.secondArgName = name;
        return this;
    }

    public Parser with(String argument) {
        switch(position) {
            case 0:
                this.command = argument;
                break;
            case 1:
                pairs.put(firstArgName, argument);
                break;
            case 2:
                pairs.put(secondArgName, argument);
                break;
            default:
                pairs.put("arg" + position, argument);
                break;
        }
        position++;
        return this;
    }}
