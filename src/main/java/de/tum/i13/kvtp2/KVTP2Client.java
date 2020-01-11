package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KVTP2Client {


    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;

    private final KVTP2Transport transport;
    private Encoder encoder;
    private Decoder decoder;
    private boolean connected;

    public KVTP2Client(String address, int port) {
        this(new BlockingKVTP2Transport(address, port));
        this.encoder = new Base64Encoder();
        this.decoder = new Base64Decoder();
        this.connected = false;
    }

    public KVTP2Client(KVTP2Transport tp) {
        this.transport = tp;
    }

    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }

    public void setEncoder(Encoder encoder) {
        this.encoder = encoder;
    }

    public boolean isConnected() {
        return connected;
    }

    public void connect() throws IOException {
        this.transport.connect();
        connected = true;
    }

    public void close() throws IOException {
        this.transport.close();
        connected = false;
    }

    public Message send(Message m) throws IOException {
        String sendVal = m.toString();
        if (m.getVersion() != Message.Version.V1) {
            sendVal = encoder.encode(sendVal, ENCODING);
        }
        String response = this.transport.send(sendVal);
        String decodedResponse = decoder.decode(response, ENCODING);
        try {
            return Message.parse(decodedResponse);
        } catch (MalformedMessageException e) {
            Message error = new Message("_error");
            error.put("msg", "malformed message");
            error.put("original", decodedResponse);
            return error;
        }
    }
}
