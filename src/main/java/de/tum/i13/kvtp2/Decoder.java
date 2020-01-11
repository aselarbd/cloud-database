package de.tum.i13.kvtp2;

import java.nio.charset.Charset;

public interface Decoder {
    String decode(String encoded, Charset charset);
}
