package de.tum.i13.kvtp2;

import java.io.*;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BlockingKVTP2Transport implements KVTP2Transport {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private final String address;
    private final int port;

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;


    BlockingKVTP2Transport(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws IOException {
        socket = new Socket(address, port);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), true, ENCODING);
    }

    @Override
    public String send(String request) throws IOException {
        out.println(request);
        return in.readLine();
    }

    @Override
    public void close() throws IOException {
        in.close();
        out.close();
        socket.close();
    }
}
