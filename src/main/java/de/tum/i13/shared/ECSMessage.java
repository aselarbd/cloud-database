package de.tum.i13.shared;

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

    }
}
