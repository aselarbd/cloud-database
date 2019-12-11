package de.tum.i13.server.ecs.handlers;

import de.tum.i13.kvtp2.KVTP2Client;

@FunctionalInterface
public interface KVTP2ClientFactory {
    KVTP2Client get(String address, int port);
}
