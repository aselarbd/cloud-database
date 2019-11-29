package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KVTP2Client {


    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;

    private final KVTP2Transport transport;
    private Encoder encoder;
    private Decoder decoder;

    public KVTP2Client(String address, int port) {
        this(new BlockingKVTP2Transport(address, port));
        this.encoder = new Base64Encoder();
        this.decoder = new Base64Decoder();
    }

    public KVTP2Client(KVTP2Transport tp) {
        this.transport = tp;
    }

    public void connect() throws IOException {
        this.transport.connect();
    }

    public Message send(Message m) throws IOException {
        String encoded = encoder.encode(m.toString());
        String response = this.transport.send(encoded);
        String decodedResponse = decoder.decode(response, ENCODING);
        return Message.parse(decodedResponse);
    }
//
//    public void send(Message request, Consumer<Message> responseHandler) {
//        this.transport.transport(request);
//    }
//
//    public void receive(byte[] data) {
//
//    }
}
