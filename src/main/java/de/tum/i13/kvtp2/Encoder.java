package de.tum.i13.kvtp2;

import java.nio.charset.Charset;

public interface Encoder {
    byte[] encode(byte[] decoded);

    default String encode(String decoded) {
        return encode(decoded, Charset.defaultCharset());
    }

    String encode(String decoded, Charset charset);
}
