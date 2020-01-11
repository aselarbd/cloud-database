package de.tum.i13.kvtp2;

import java.nio.charset.Charset;
import java.util.Base64;

public class Base64Encoder implements Encoder {

    byte[] encode(byte[] decoded) {
        return Base64.getEncoder().encode(decoded);
    }

    @Override
    public String encode(String decoded, Charset charset) {
        return "b64:" + new String(encode(decoded.getBytes(charset)), charset);
    }
}
