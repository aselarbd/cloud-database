package de.tum.i13.kvtp2;

import java.nio.charset.Charset;
import java.util.Base64;

public class Base64Decoder implements Decoder {
    @Override
    public byte[] decode(byte[] encoded) {
        return Base64.getDecoder().decode(encoded);
    }

    @Override
    public String decode(String encoded, Charset charset) {
        return new String(decode(encoded.getBytes(charset)), charset);
    }
}
