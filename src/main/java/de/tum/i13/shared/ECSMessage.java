package de.tum.i13.shared;

import java.util.ArrayList;
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

    protected void checkLength(int index) throws IndexOutOfBoundsException {
        if (index < 0 || !isVariable && index >= arguments.size()) {
            throw new IndexOutOfBoundsException();
        }
    }

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

    public void addIpPort(int index, String ip, int port)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        checkLength(index);
        expectIpPort(index);
        // checks passed, now set the value
        arguments.add(index, ip + ":" + Integer.toString(port));
    }

}
