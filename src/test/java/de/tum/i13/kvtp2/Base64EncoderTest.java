package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class Base64EncoderTest {

    @Test
    void testEncodeString() {
        Encoder encoder = new Base64Encoder();
        String encoded = encoder.encode("hello world\n", StandardCharsets.UTF_8);
        assertThat(encoded, is(equalTo("b64:aGVsbG8gd29ybGQK")));
    }
}