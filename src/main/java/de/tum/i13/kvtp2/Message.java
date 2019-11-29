package de.tum.i13.kvtp2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Message {

    private Parser getInstance() {
        Parser parser = new Parser() {


        };
        return parser.with(Type.RESPONSE).with("keyrange_read_success");
    }

    private static Map<String, Factory<Parser>> oldStyleKeyWords = new HashMap<>() {
        {
            put("put", () -> new Parser().with(Type.REQUEST));
            put("get", () -> new Parser().with(Type.REQUEST));
            put("delete", () -> new Parser().with(Type.REQUEST));
            put("keyrange", () -> new Parser().with(Type.REQUEST));
            put("keyrange_read", () -> new Parser().with(Type.REQUEST));
            put("put_success", () -> new Parser().with(Type.RESPONSE));
            put("put_update", () -> new Parser().with(Type.RESPONSE));
            put("put_error", () -> new Parser().with(Type.RESPONSE));
            put("server_stopped", () -> new Parser().with(Type.RESPONSE));
            put("server_write_lock", () -> new Parser().with(Type.RESPONSE));
            put("get_error", () -> new GetErrorParser().with(Type.RESPONSE).withSecondArgName("msg"));
            put("get_success", () -> new Parser().with(Type.RESPONSE));
            put("delete_success", () -> new Parser().with(Type.RESPONSE));
            put("delete_error", () -> new Parser().with(Type.RESPONSE));
            put("keyrange_success", () -> new KeyrangeParser().with(Type.RESPONSE).withFirstArgName("keyrange"));
            put("keyrange_read_success", () -> new KeyrangeParser().with(Type.RESPONSE).withFirstArgName("keyrange"));
            put("error", () -> new Parser().with(Type.RESPONSE).withFirstArgName("description"));
        }
    };

    private static Pattern beginsWithOldStyle = Pattern.compile(
            "^(" + String.join("|", oldStyleKeyWords.keySet()) + ").*$"
    );

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
        if (id == -1) {
            return oldStyleToString();
        }
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

    private String oldStyleToString() {
        String joined = String.join(" ", pairs.values());
        return String.join(" ", command, joined).trim();
    }

    public static Message parse(String msg) {
        String[] lines = msg.split("\\R");

        Matcher matcher = beginsWithOldStyle.matcher(lines[0]);
        if (matcher.matches()) {
            return parseOldStyleMessage(msg);
        }

        Map<String, String> values = new HashMap<>();
        for (String line : lines) {
            String[] kv = line.split(":");
            if (kv.length < 2) {
                throw new IllegalArgumentException("Invalid kv pair at " + line);
            }
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

    private static Message parseOldStyleMessage(String input) {
        String[] parts = input.split("\\s+");
        Parser parser = oldStyleKeyWords.get(parts[0]).getInstance();
        if (parser == null) {
            throw new IllegalArgumentException("Invalid old style message " + input);
        }
        for (String part : parts) {
            parser.with(part);
        }
        Message msg = parser.parse();
        msg.setID(-1);
        return msg;
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
