package de.tum.i13.kvtp2;

import java.io.*;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BlockingTransport implements KVTP2Transport {

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;
    private final String address;
    private final int port;

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;


    public BlockingTransport(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws IOException {
        this.socket = new Socket(address, port);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), true, ENCODING);
    }

    @Override
    public String send(String request) throws IOException {
        out.println(request);
        return in.readLine();
    }
}
