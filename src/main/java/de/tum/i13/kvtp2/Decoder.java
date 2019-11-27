package de.tum.i13.kvtp2;

import java.nio.charset.Charset;

public interface Decoder {
    byte[] decode(byte[] encoded);

    default String decode(String encoded) {
        return decode(encoded, Charset.defaultCharset());
    }

    String decode(String encoded, Charset charset);
}
