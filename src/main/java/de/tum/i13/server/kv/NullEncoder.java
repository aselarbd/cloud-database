package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.Encoder;

import java.nio.charset.Charset;

public class NullEncoder implements Encoder {
    @Override
    public byte[] encode(byte[] decoded) {
        return decoded;
    }

    @Override
    public String encode(String decoded, Charset charset) {
        return decoded;
    }
}
