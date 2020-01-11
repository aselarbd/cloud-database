package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class Base64DecoderTest {

    @Test
    void testDecodeString() {
        Decoder decoder = new Base64Decoder();
        String decoded = decoder.decode("b64:aGVsbG8gd29ybGQK", StandardCharsets.UTF_8);
        assertThat(decoded, is(equalTo("hello world\n")));
        // leave values without b64: prefix unchanged
        decoded = decoder.decode("test", StandardCharsets.UTF_8);
        assertThat(decoded, is(equalTo("test")));
    }
}