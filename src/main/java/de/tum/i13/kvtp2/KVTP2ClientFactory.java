package de.tum.i13.kvtp2;

@FunctionalInterface
public interface KVTP2ClientFactory {
    KVTP2Client get(String address, int port);
}
