package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Describes a message to or from the ECS.
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
         * Like IP_PORT, but may contain an arbitrary number of ip:port elements.
         * These elements are separated via spaces.
         *
         * This element may only occur at the end of an argument list.
         */
        IP_PORT_VARNUM
    }

    /**
     * Possible ECS messages and information on their expected arguments.
     *
     * The arguments are given as a List of {@link MsgArg}. The length usually determines
     * the expected number of arguments, with the exception of IP_PORT_VARNUM. If this occurs, the
     * length is to be interpreted as minimum length.
     */
    public enum MsgType {
        RESPONSE_OK("ok", new MsgArg[]{}),
        RESPONSE_ERROR("error", new MsgArg[]{}),
        WRITE_LOCK("write_lock", new MsgArg[]{}),
        REL_LOCK("release_lock", new MsgArg[]{}),
        REGISTER_SERVER("register", new MsgArg[]{}),
        NEXT_ADDR("next_addr", new MsgArg[]{MsgArg.IP_PORT}),
        TRANSFER_RANGE("transfer_range", new MsgArg[]{MsgArg.IP_PORT,
            MsgArg.IP_PORT, MsgArg.IP_PORT}),
        ECS_PUT("ecs_put", new MsgArg[]{MsgArg.BASE64_STR}),
        PUT_DONE("done", new MsgArg[]{}),
        BROADCAST_NEW("broadcast_new", new MsgArg[]{MsgArg.IP_PORT}),
        BROADCAST_REM("broadcast_rem", new MsgArg[]{MsgArg.IP_PORT}),
        KEYRANGE("keyrange", new MsgArg[]{MsgArg.IP_PORT_VARNUM}),
        ANNOUNCE_SHUTDOWN("announce_shutdown", new MsgArg[]{}),
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

        public boolean hasVariableArgs() {
            for (MsgArg arg : args) {
                if (arg == MsgArg.IP_PORT_VARNUM) {
                    return true;
                }
            }
            return false;
        }
    }

    private MsgType messageType;
    // use a list to be able to handle dynamic arguments
    private List<String> arguments;
    private boolean isVariable;

    public ECSMessage(MsgType type) {
        this.messageType = type;
        this.isVariable = type.hasVariableArgs();
        if (this.isVariable || type.getArgs().length == 0) {
            this.arguments = new ArrayList<>();
        } else {
            this.arguments = new ArrayList<>(type.getArgs().length);
        }
    }

    /**
     * Helper to check if the given index is valid for this item.
     *
     * @param index
     *  Index to perform an operation on
     * @param write
     *  true if data should be written at the index
     * @throws IndexOutOfBoundsException if the index is not valid
     */
    protected void checkLength(int index, boolean write) throws IndexOutOfBoundsException {
        if (index < 0 || index > arguments.size()) {
            throw new IndexOutOfBoundsException();
        } else if (index == arguments.size() && (!write || !isVariable)) {
            // when reading or having fixed args, index may not be equal to size as well,
            // this is only valid for appending variable args
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Check if the message type of this instance expects IP:Port at the given index.
     *
     * @param index Index to check
     * @throws IllegalArgumentException if no IP:Port is allowed here
     */
    protected void expectIpPort(int index) throws IllegalArgumentException {
        // variable args are expected to be at the end.
        int varStartIndex = messageType.getArgs().length - 1;
        if (isVariable && index >= varStartIndex) {
            MsgArg lastArg = messageType.getArgs()[varStartIndex];
            if (lastArg != MsgArg.IP_PORT_VARNUM) {
                throw new IllegalArgumentException("IP:Port argument not allowed at this position");
            }
        } else {
            // either fixed args or before start of variable range. Expect IP_PORT.
            if (messageType.getArgs()[index] != MsgArg.IP_PORT) {
                throw new IllegalArgumentException("IP:Port argument not allowed at this position");
            }
        }
    }

    /**
     * Check if the message type of this instance allows a Base64 String at the given index.
     *
     * @param index Index to check
     * @throws IllegalArgumentException if no Base64 String is allowed at the given position.
     */
    protected void expectBase64(int index) throws IllegalArgumentException {
        if (messageType.getArgs()[index] != MsgArg.BASE64_STR) {
            throw new IllegalArgumentException("Base64 string not allowed at this position");
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
        checkLength(index, true);
        expectIpPort(index);
        // checks passed, now set the value
        arguments.add(index, addr.getHostString() + ":" + addr.getPort());
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
        checkLength(index, false);
        expectIpPort(index);
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
        checkLength(index, true);
        expectBase64(index);
        arguments.add(index, new String(Base64.getEncoder().encode(cleartext.getBytes())));
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
        checkLength(index, false);
        expectBase64(index);
        return new String(Base64.getDecoder().decode(
                arguments.get(index).getBytes()
        ));
    }

    /**
     * Constructs a string which is ready for transmission out of the raw data.
     *
     * @return Message to be sent
     */
    public String getFullMessage() {
        // TODO: perform checks if message is complete
        String argsStr = (arguments.size() > 0) ? " " + String.join(" ", arguments) : "";
        return messageType.getMsgName() + argsStr;
    }
}
