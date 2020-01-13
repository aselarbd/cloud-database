package de.tum.i13.kvtp2;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class Message provides a datatype for abstracting the information
 * which is send over the network. It provides two versions for serialization
 * and parsing. V1 is compatible with the public API specification of KV-Servers
 * and KV-Clients, V2 provides a simple Key-Value message format.
 *
 * Messages have some Metadata and some content. The metadata includes:
 * an ID - to match responses to requests
 * a Version - V1 or V2 as described above
 * a type - Request/Response
 * a command - which is used to route the message to the correct handler in a server
 */
public class Message {

    private static final String KEY_VALUE_DELIMITER = ":";

    /**
     * oldStyleKeyWords provides the API Messages for V1 and defines the parser
     * which is used for each version.
     */
    private static final Map<String, Supplier<Parser>> oldStyleKeyWords = new HashMap<>() {
        {
            put("put", () -> new PutParser().with(Type.REQUEST).withFullText());
            put("get", () -> new Parser().with(Type.REQUEST));
            put("delete", () -> new Parser().with(Type.REQUEST));
            put("keyrange", () -> new Parser().with(Type.REQUEST));
            put("keyrange_read", () -> new Parser().with(Type.REQUEST));
            put("serverLogLevel", ()-> new Parser().with(Type.REQUEST).withFirstArgName("level"));
            put("scan", () -> new Parser().with(Type.REQUEST).withFirstArgName("partialKey"));
            put("put_success", () -> new Parser().with(Type.RESPONSE));
            put("put_update", () -> new Parser().with(Type.RESPONSE));
            put("put_error", () -> new Parser().with(Type.RESPONSE));
            put("server_stopped", () -> new Parser().with(Type.RESPONSE));
            put("server_not_responsible", () -> new Parser().with(Type.RESPONSE));
            put("server_write_lock", () -> new Parser().with(Type.RESPONSE));
            put("get_error", () -> new GetErrorParser().with(Type.RESPONSE).withSecondArgName("msg"));
            put("get_success", () -> new Parser().with(Type.RESPONSE));
            put("delete_success", () -> new Parser().with(Type.RESPONSE));
            put("delete_error", () -> new Parser().with(Type.RESPONSE));
            put("scan_success", () -> new Parser().with(Type.RESPONSE));
            put("scan_error", () -> new GetErrorParser().with(Type.RESPONSE).withSecondArgName("msg"));
            put("keyrange_success", () -> new KeyrangeParser().with(Type.RESPONSE).withFirstArgName("keyrange"));
            put("keyrange_read_success", () -> new KeyrangeParser().with(Type.RESPONSE).withFirstArgName("keyrange"));
            put("error", () -> new Parser().with(Type.RESPONSE).withFirstArgName("description"));
            put("connected", () -> new Parser().with(Type.REQUEST));
        }
    };

    private static final Pattern beginsWithOldStyle = Pattern.compile(
            "^(" + String.join("|", oldStyleKeyWords.keySet()) + ").*$"
    );

    /**
     * Type determines, whether the message is a request or a response to a request.
     */
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

    private InetSocketAddress src;

    private final Map<String, String> pairs = new LinkedHashMap<>();

    /**
     * Creates a new V2 message with a given command.
     *
     * @param command The default command for this message. Can be changed by a separate setter
     */
    public Message(String command) {
        this(command, Version.V2);
    }

    /**
     * Creates a new Message with a given command and version
     *
     * @param command The default command for this message. Can be changed by a separate setter
     * @param version The version to be used for the new message.
     */
    public Message(String command, Version version) {
        this.id = getNextID();
        this.type = Type.REQUEST;
        this.command = command;
        this.version = version;
    }

    private int getNextID() {
        return nextID++;
    }

    /**
     * Add a new key-value pair to the message
     *
     * @param key key
     * @param value value
     */
    public void put(String key, String value) {
        pairs.put(key, value);
    }

    /**
     * Get the message body as string
     *
     * @return
     */
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

    /**
     * Parse a string as a message. Message Version is set by a regular expression.
     * If the first word matches a keyword of the public KV-Server API, V1 is used,
     * otherwise V2.
     *
     * @param msg Message to parse
     * @return A new Message object, containing the key-value pairs taken from the string representation
     * @throws MalformedMessageException If a message has a bad format
     */
    public static Message parse(String msg) throws MalformedMessageException {
        String[] lines = msg.split("\\R");

        Matcher matcher = beginsWithOldStyle.matcher(lines[0]);
        if (matcher.matches()) {
            return parseOldStyleMessage(msg);
        }

        Map<String, String> values = new HashMap<>();
        for (String line : lines) {
            String[] kv = line.split(KEY_VALUE_DELIMITER, 2);
            if (kv.length != 2) {
                throw new MalformedMessageException("Invalid kv pair at " + line);
            }
            values.put(kv[0], kv[1]);
        }

        Message m = new Message(values.get("_command"));
        m.setType(Type.valueOf(values.get("_type")));
        m.setID(Integer.parseInt(values.get("_id")));
        m.setVersion(Version.valueOf(values.get("_version")));

        values.remove("_type");
        values.remove("_id");
        values.remove("_command");
        values.remove("_version");

        values.forEach(m::put);
        return m;
    }

    private static Message parseOldStyleMessage(String input) throws MalformedMessageException {
        String[] parts = input.split("\\s+");
        Parser parser = oldStyleKeyWords.get(parts[0]).get();
        if (parser == null) {
            throw new IllegalArgumentException("Invalid old style message " + input);
        }
        if (parser.needsFullText()) {
            parser = parser.withFullText(input);
        } else {
            for (String part : parts) {
                parser = parser.with(part);
            }
        }
        Message msg = parser.parse();
        msg.setVersion(Version.V1);
        return msg;
    }

    /**
     * Convenience method to get a response for a given request (which has the same message id)
     * @param request to get a response for
     * @return a new response with the same request id as the request
     */
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

    public void setType(Type type) {
        this.type = type;
    }

    public int getID() {
        return id;
    }

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

    public InetSocketAddress getSrc() {
        return src;
    }

    public void setSrc(InetSocketAddress src) {
        this.src = src;
    }
}
