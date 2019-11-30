package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.charset.Charset;

class EncodedMessageWriter implements MessageWriter {
    private final StringWriter responseWriter;
    private final Encoder encoder;
    private final Charset encoding;

    public EncodedMessageWriter(StringWriter responseWriter, Encoder encoder, Charset encoding) {
        this.responseWriter = responseWriter;
        this.encoder = encoder;
        this.encoding = encoding;
    }

    @Override
    public void write(Message message) {
        String encodedMessage = encoder.encode(message.toString(), encoding);
        responseWriter.write(encodedMessage);
    }

    @Override
    public void flush() {
        responseWriter.flush();
    }

    @Override
    public void close() throws IOException {
        responseWriter.close();
    }
}
