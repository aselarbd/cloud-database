package de.tum.i13.kvtp2;

import java.nio.charset.Charset;

public interface Encoder {
    String encode(String decoded, Charset charset);
}
