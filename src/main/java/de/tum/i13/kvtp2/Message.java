package de.tum.i13.kvtp2;

import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Message {

    private static final String KEY_VALUE_DELIMITER = "##";

    private static Map<String, Supplier<Parser>> oldStyleKeyWords = new HashMap<>() {
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

    public enum Version {
        V1,
        V2
    }

    private static int nextID = 1;

    private int id;
    private Version version;
    private Type type;
    private String command;

    private Map<String, String> pairs = new LinkedHashMap<>();

    @Deprecated
    public Message(Type t, String command) {
        this.id = getNextID();
        this.type = t;
        this.command = command;
        this.version = Version.V2;
    }

    public Message(String command) {
        this.id = getNextID();
        this.type = Type.REQUEST;
        this.command = command;
        this.version = Version.V2;
    }

    private int getNextID() {
        return nextID++;
    }

    public void put(String key, String value) {
        pairs.put(key, value);
    }

    public String body() {
        StringBuilder sb = new StringBuilder();
        pairs.forEach((k, v) -> sb.append(k)
                                    .append(KEY_VALUE_DELIMITER)
                                    .append(v)
                                    .append("\r\n"));
        return sb.toString();
    }

    @Override
    public String toString() {
        if (version == Version.V1) {
            return oldStyleToString();
        }
        return  "_id" + KEY_VALUE_DELIMITER + id + "\r\n" +
                "_version" + KEY_VALUE_DELIMITER + version + "\r\n" +
                "_type" + KEY_VALUE_DELIMITER + type + "\r\n" +
                "_command" + KEY_VALUE_DELIMITER + command + "\r\n" +
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
            String[] kv = line.split(KEY_VALUE_DELIMITER);
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid kv pair at " + line);
            }
            values.put(kv[0], kv[1]);
        }

        Message m = new Message(Type.valueOf(values.get("_type")), values.get("_command"));
        m.setID(Integer.parseInt(values.get("_id")));
        m.setVersion(Version.valueOf(values.get("_version")));

        values.remove("_type");
        values.remove("_id");
        values.remove("_command");

        values.forEach(m::put);
        return m;
    }

    private static Message parseOldStyleMessage(String input) {
        String[] parts = input.split("\\s+");
        Parser parser = oldStyleKeyWords.get(parts[0]).get();
        if (parser == null) {
            throw new IllegalArgumentException("Invalid old style message " + input);
        }
        for (String part : parts) {
            parser.with(part);
        }
        Message msg = parser.parse();
        msg.setVersion(Version.V1);
        return msg;
    }

    public static Message getResponse(Message request) {
        Message response = new Message(request.getCommand());
        response.type = Type.RESPONSE;
        response.id = request.id;
        response.setVersion(request.getVersion());
        return response;
    }

    public Type getType() {
        return type;
    }

    public int getID() {
        return id;
    }

    @Deprecated
    public void setID(int id) {
        this.id = id;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public String getCommand() {
        return this.command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String get(String key) {
        return pairs.get(key);
    }
}
