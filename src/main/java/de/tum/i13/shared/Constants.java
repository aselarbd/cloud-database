package de.tum.i13.shared;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Constants {
	public static final String TELNET_ENCODING = "ISO-8859-1"; // encoding for telnet
    public static final String DELETE_MARKER = "KVSTORE::DELETE_MARKER";
    public static final String FLUSH_MESSAGE = "KVSTORE::CACHE_FLUSH";
    public static final String REPLICATION_STOP_MARKER = "KVSTORE::REPLICATION_STOP_MARKER";

    public static final int PING_TIMEOUT = 700;

    public static final Charset TELNET_ENCODING_CHARSET = StandardCharsets.ISO_8859_1;
}
