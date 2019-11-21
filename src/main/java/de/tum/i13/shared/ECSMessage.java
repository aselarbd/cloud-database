package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Describes a message to or from the ECS.
 *
 * For constructing a message, you have to initially call the add methods with the respective types in the
 * order of the arguments (i.e. it isn't possible to create argument 1 before argument 0). This ensures that
 * all arguments are present and have the correct type.
 *
 * After elements are initialized, updates are possible in any order via the same add calls.
 *
 * If the message contains all required items with the proper types, a string representation can be generated.
 */
public class ECSMessage {
    /**
     * Argument for an ECS message
     */
    public enum MsgArg {
        /**
         * IP and port as String, denoted as ip:port
         */
        IP_PORT,
        /**
         * A base64 String
         */
        BASE64_STR,
        /**
         * Key range string in the format used by keyrange messages between clients and servers,
         * i.e. comma-separated start hash, end hash, ip values, which are then separated via
         * semicolons.
         */
        KEYRANGE
    }

    /**
     * Possible ECS messages and information on their expected arguments.
     *
     * The arguments are given as a List of {@link MsgArg}. The length determines
     * the expected number of arguments.
     */
    public enum MsgType {
        RESPONSE_OK("ok", new MsgArg[]{}),
        RESPONSE_ERROR("error", new MsgArg[]{}),
        WRITE_LOCK("write_lock", new MsgArg[]{}),
        REL_LOCK("release_lock", new MsgArg[]{}),
        REGISTER_SERVER("register", new MsgArg[]{MsgArg.IP_PORT}),
        NEXT_ADDR("next_addr", new MsgArg[]{MsgArg.IP_PORT}),
        TRANSFER_RANGE("transfer_range", new MsgArg[]{MsgArg.IP_PORT,
            MsgArg.IP_PORT, MsgArg.IP_PORT}),
        ECS_PUT("ecs_put", new MsgArg[]{MsgArg.BASE64_STR}),
        PUT_DONE("done", new MsgArg[]{}),
        BROADCAST_NEW("broadcast_new", new MsgArg[]{MsgArg.IP_PORT}),
        BROADCAST_REM("broadcast_rem", new MsgArg[]{MsgArg.IP_PORT}),
        KEYRANGE("keyrange", new MsgArg[]{MsgArg.KEYRANGE}),
        ANNOUNCE_SHUTDOWN("announce_shutdown", new MsgArg[]{MsgArg.IP_PORT}),
        PING("ping", new MsgArg[]{}),
        ;

        private final String msgName;
        private final MsgArg[] args;

        MsgType(String msgName, MsgArg[] args) {
            this.msgName = msgName;
            this.args = args;
        }

        public String getMsgName() {
            return msgName;
        }

        public MsgArg[] getArgs() {
            return args;
        }
    }

    private MsgType messageType;
    // use a list to be able to handle dynamic arguments
    private List<String> arguments;

    public ECSMessage(MsgType type) {
        this.messageType = type;
        if (type.getArgs().length == 0) {
            this.arguments = new ArrayList<>();
        } else {
            this.arguments = new ArrayList<>(type.getArgs().length);
        }
    }

    /**
     * Generates a new instance based on a raw argument string. This is intended for parsing.
     * Use the constructor taking type only to build messages.
     *
     * @param type Expected message type
     * @param rawArgs Arguments to be parsed
     * @throws IllegalArgumentException if the arguments do not match the message type
     */
    public ECSMessage(MsgType type, String[] rawArgs)
            throws IllegalArgumentException {
        this(type);
        if (rawArgs.length != type.args.length) {
            throw new IllegalArgumentException("Non-matching argument count");
        }
        for (int i = 0; i < rawArgs.length; i++) {
            this.arguments.add(rawArgs[i]);
            // try to interpret the values according to their type. Abort if something goes wrong,
            // otherwise assume the raw data is valid.
            try {
                switch (type.args[i]) {
                    case IP_PORT:
                        getIpPort(i);
                        break;
                    case BASE64_STR:
                        getBase64(i);
                        break;
                    case KEYRANGE:
                        getKeyrange(i);
                        break;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not construct message", e);
            }
        }
    }

    /**
     * Gets the message type of this instance.
     *
     * @return The {@link ECSMessage.MsgType}
     */
    public ECSMessage.MsgType getType() {
        return this.messageType;
    }

    /**
     * Appends or updates an argument to the argument list.
     *
     * @param index Index to add the item at
     * @param item Argument as String
     */
    private void addOrUpdate(int index, String item) {
        if (index == arguments.size()) {
            arguments.add(item);
        } else {
            arguments.set(index, item);
        }
    }

    /**
     * Helper to check if the given index is valid for this item.
     *
     * @param index
     *  Index to perform an operation on
     * @throws IndexOutOfBoundsException if the index is not valid
     */
    protected void checkLength(int index) throws IndexOutOfBoundsException {
        if (index < 0 || index > arguments.size() || index >= messageType.getArgs().length) {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Check if the message type of this instance expects the given arg type at the given index.
     *
     * @param index Index to check
     * @param arg Expected argument type
     * @throws IllegalArgumentException if the type is not allowed here
     */
    protected void expectAt(int index, MsgArg arg) throws IllegalArgumentException {
        if (messageType.getArgs()[index] != arg) {
            throw new IllegalArgumentException("Given argument type not allowed at this position");
        }
    }

    /**
     * Adds or modifies an IP:Port value at the given index.
     *
     * @param index
     *  Index to add or modify
     * @param addr
     *  IP:Port value as InetSocketAddress
     * @throws IndexOutOfBoundsException if the index is invalid
     * @throws IllegalArgumentException if no IP:Port is allowed at the given position
     */
    public void addIpPort(int index, InetSocketAddress addr)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        checkLength(index);
        expectAt(index, MsgArg.IP_PORT);
        // checks passed, now set the value
        addOrUpdate(index, addr.getHostString() + ":" + addr.getPort());
    }

    /**
     * Gets the IP:Port value at the given index as InetSocketAddress.
     *
     * @param index Index to read from
     * @return Value parsed as InetSocketAddress
     * @throws IndexOutOfBoundsException if the index is invalid
     * @throws IllegalArgumentException if no IP:Port is allowed at the given position or
     *  if the value can't be parsed as InetSocketAddress
     */
    public InetSocketAddress getIpPort(int index)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        checkLength(index);
        expectAt(index, MsgArg.IP_PORT);
        InetSocketAddressTypeConverter converter = new InetSocketAddressTypeConverter();
        try {
            return converter.convert(arguments.get(index));
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Adds a Base64 string at the given position
     *
     * @param index
     *  Index to add or modify
     * @param cleartext
     *  String to add. This is the cleartext value without Base64 encoding.
     * @throws IndexOutOfBoundsException If the index is invalid
     * @throws IllegalArgumentException if there is no Base64 string allowed at this position
     */
    public void addBase64(int index, String cleartext)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        checkLength(index);
        expectAt(index, MsgArg.BASE64_STR);
        addOrUpdate(index, new String(Base64.getEncoder().encode(cleartext.getBytes())));
    }

    /**
     * Reads the value at the given position as Base64 String.
     *
     * @param index Index to read from
     * @return The decoded value
     * @throws IndexOutOfBoundsException If the index is invalid
     * @throws IllegalArgumentException If there is no Base64 string allowed at this position
     */
    public String getBase64(int index)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        checkLength(index);
        expectAt(index, MsgArg.BASE64_STR);
        return new String(Base64.getDecoder().decode(
                arguments.get(index).getBytes()
        ));
    }

    /**
     * Add a keyrange argument. This is done by directly giving a {@link ConsistentHashMap} instance.
     *
     * @param index
     *  Argument index
     * @param map
     *  Hashmap instance to get the argument from
     * @throws IndexOutOfBoundsException if the index is invalid
     * @throws IllegalArgumentException if there is no KEYRANGE item allowed at this position
     */
    public void addKeyrange(int index, ConsistentHashMap map)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        checkLength(index);
        expectAt(index, MsgArg.KEYRANGE);
        addOrUpdate(index, map.getKeyrangeString());
    }

    /**
     * Gets a keyrange argument. The raw argument is parsed as {@link ConsistentHashMap}.
     *
     * @param index The index of the argument
     * @return A new {@link ConsistentHashMap} based on the argument values
     * @throws IndexOutOfBoundsException if the index is invalid
     * @throws IllegalArgumentException if there is no KEYRANGE item allowed at this position or if parsing failed
     * @throws NoSuchAlgorithmException if MD5 is unavailable (should never happen)
     */
    public ConsistentHashMap getKeyrange(int index)
            throws IndexOutOfBoundsException, IllegalArgumentException, NoSuchAlgorithmException {
        checkLength(index);
        expectAt(index, MsgArg.KEYRANGE);
        return ConsistentHashMap.fromKeyrangeString(arguments.get(index));
    }

    /**
     * Constructs a string which is ready for transmission out of the raw data.
     *
     * @return Message to be sent, or an empty string if the data is incomplete.
     */
    public String getFullMessage() {
        if (arguments.size() != messageType.getArgs().length) {
            return "";
        }
        String argsStr = (arguments.size() > 0) ? " " + String.join(" ", arguments) : "";
        return messageType.getMsgName() + argsStr;
    }
}
