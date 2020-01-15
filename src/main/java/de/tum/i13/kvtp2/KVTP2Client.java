package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * KVTP2Client provides a blocking client implementation for the kvtp2 protocol
 */
public class KVTP2Client {


    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;

    private final KVTP2Transport transport;
    private Encoder encoder;
    private Decoder decoder;
    private boolean connected;

    /**
     * Get a new client for communication to the server at address:port
     *
     * @param address the address this client can connect to
     * @param port the port of the server, to which this client will connect
     */
    public KVTP2Client(String address, int port) {
        this(new BlockingKVTP2Transport(address, port));
        this.encoder = new Base64Encoder();
        this.decoder = new Base64Decoder();
        this.connected = false;
    }

    /**
     * Create a new Client with a custom transport implementation
     * This doesn't set the default de-/encoders, you have to set it explicitly
     * if you want to use any de-/encoding.
     *
     * @param tp transport to use in this client
     */
    public KVTP2Client(KVTP2Transport tp) {
        this.transport = tp;
    }

    /**
     * Set a different decoder for this client (Default is base64)
     *
     * @param decoder decoder to use
     */
    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    /**
     * Set a different encoder for this client (Default is base64)
     *
     * @param encoder encoder to use
     */
    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * Connect to the server
     *
     * @throws IOException If an I/O error occurs
     */
    public void connect() throws IOException {
        this.transport.connect();
        connected = true;
    }

    /**
     * Close the connection
     *
     * @throws IOException If an I/O error occurs
     */
    public void close() throws IOException {
        this.transport.close();
        connected = false;
    }

    /**
     * Send a message to the connected server
     *
     * @param m Message to send
     * @return The response which was received for the request or an error message
     * @throws IOException If an I/O error occurs
     */
    public Message send(Message m) throws IOException {
        String sendVal = m.toString();
        if (m.getVersion() != Message.Version.V1) {
            sendVal = encoder.encode(sendVal, ENCODING);
        }
        String response = this.transport.send(sendVal);
        Message error = new Message("_error");
        if (response == null) {
            error.put("msg", "empty message");
            return error;
        }
        String decodedResponse = decoder.decode(response, ENCODING);
        try {
            return Message.parse(decodedResponse);
        } catch (MalformedMessageException e) {
            error.put("msg", "malformed message");
            error.put("original", decodedResponse);
            return error;
        }
    }
}
