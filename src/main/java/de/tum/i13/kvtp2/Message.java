package de.tum.i13.kvtp2;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Message {

    public enum Type {
        REQUEST,
        RESPONSE;

        @Override
        public String toString() {
            return super.toString().stripLeading().stripTrailing();
        }
    }

    private static int nextID = 1;

    private int id;
    private Type type;
    private String command;

    private Map<String, String> pairs = new LinkedHashMap<>();

    public Message(Type t, String command) {
        id = nextID;
        nextID++;
        this.type = t;
        this.command = command;
    }

    public void put(String key, String value) {
        pairs.put(key, value);
    }

    public String body() {
        StringBuilder sb = new StringBuilder();
        pairs.forEach((k, v) -> sb.append(k)
                                    .append(":")
                                    .append(v)
                                    .append("\r\n"));
        return sb.toString();
    }

    @Override
    public String toString() {
        return  "_id:" +
                id +
                "\r\n" +
                "_type:" +
                type +
                "\r\n" +
                "_command:" +
                command +
                "\r\n" +
                body();
    }

    public static Message parse(String msg) {
        String[] lines = msg.split("\\R");

        Map<String, String> values = new HashMap<>();
        for (String line : lines) {
            String[] kv = line.split(":");
            values.put(kv[0], kv[1]);
        }

        Message m = new Message(Type.valueOf(values.get("_type")), values.get("_command"));
        m.setID(Integer.parseInt(values.get("_id")));

        values.remove("_type");
        values.remove("_id");
        values.remove("_command");

        values.forEach(m::put);
        return m;
    }

    public void setType(Type t) {
        this.type = t;
    }

    public Type getType() {
        return type;
    }

    public int getID() {
        return id;
    }

    private void setID(int id) {
        this.id = id;
    }

    public String getCommand() {
        return this.command;
    }

    public String get(String key) {
        return pairs.get(key);
    }
}
