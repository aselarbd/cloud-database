package de.tum.i13.shared;

/**
 * Describes a message to or from the ECS.
 */
public class ECSMessage {
    /**
     * Arguments for an ECS message
     */
    public enum MsgArgs {
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
     * The arguments are given as a List of {@link ECSMessage.MsgArgs}. The length usually determines
     * the expected number of arguments, with the exception of IP_PORT_VARNUM. If this occurs, the
     * length is to be interpreted as minimum length.
     */
    public enum MsgType {
        RESPONSE_OK("ok", new MsgArgs[]{}),
        RESPONSE_ERROR("error", new MsgArgs[]{}),
        WRITE_LOCK("write_lock", new MsgArgs[]{}),
        REL_LOCK("release_lock", new MsgArgs[]{}),
        REGISTER_SERVER("register", new MsgArgs[]{}),
        NEXT_ADDR("next_addr", new MsgArgs[]{MsgArgs.IP_PORT}),
        TRANSFER_RANGE("transfer_range", new MsgArgs[]{MsgArgs.IP_PORT,
            MsgArgs.IP_PORT, MsgArgs.IP_PORT}),
        ECS_PUT("ecs_put", new MsgArgs[]{MsgArgs.BASE64_STR}),
        PUT_DONE("done", new MsgArgs[]{}),
        BROADCAST_NEW("broadcast_new", new MsgArgs[]{MsgArgs.IP_PORT}),
        BROADCAST_REM("broadcast_rem", new MsgArgs[]{MsgArgs.IP_PORT}),
        KEYRANGE("keyrange", new MsgArgs[]{MsgArgs.IP_PORT_VARNUM}),
        ANNOUNCE_SHUTDOWN("announce_shutdown", new MsgArgs[]{}),
        PING("ping", new MsgArgs[]{}),
        ;

        private final String msgName;
        private final MsgArgs[] args;

        MsgType(String msgName, MsgArgs[] args) {
            this.msgName = msgName;
            this.args = args;
        }

    }
}
