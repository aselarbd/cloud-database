/**
 * Package kvtp2 provides an implementation of a protocol
 * for communication over the network.
 * The package provides a high-level interface for sending
 * and receiving messages.
 *
 * The basic datatype is {@link de.tum.i13.kvtp2.Message}, which
 * provides an abstraction for all data which is send using the
 * protocol. It implements a simple key-value datatype, which
 * can be serialized in two versions {@link de.tum.i13.kvtp2.Message.Version}.
 * V1 is compatible to the API definition for KV-Server and KV-Client,
 * while V2 provides a type for more fine grained information transmission.
 *
 * The package also provides a server, a blocking and a non-blocking
 * client implementation and some Default-Middleware classes in the middleware
 * package.
 */
package de.tum.i13.kvtp2;