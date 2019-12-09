package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.Decoder;

import java.nio.charset.Charset;

public class NullDecoder implements Decoder {
    @Override
    public byte[] decode(byte[] encoded) {
        return encoded;
    }

    @Override
    public String decode(String encoded, Charset charset) {
        return encoded;
    }
}
