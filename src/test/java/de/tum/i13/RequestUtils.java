package de.tum.i13;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class RequestUtils {
    public static String doRequest(Socket s, String req) throws IOException {
        PrintWriter output = new PrintWriter(s.getOutputStream());
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));

        output.write(req + "\r\n");
        output.flush();

        return input.readLine();
    }

    public static String readMessage(Socket s) throws IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(s.getInputStream()));
        return input.readLine();
    }

}
