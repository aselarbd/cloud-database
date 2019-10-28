package de.tum.i13.server.threadperconnection;

import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.logging.Logger;

public class ConnectionHandleThread extends Thread {

    public static Logger logger = Logger.getLogger(ConnectionHandleThread.class.getName());

    private CommandProcessor cp;
    private Socket clientSocket;

    public ConnectionHandleThread(CommandProcessor commandProcessor, Socket clientSocket) {
        this.cp = commandProcessor;
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        logger.info("run new connectionHandleThread");
        try {
            logger.info("get in-/output-streams");
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), Constants.TELNET_ENCODING));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), Constants.TELNET_ENCODING));

            out.write("connected to server\r\n");
            out.flush();
            logger.info("read commands");
            String firstLine;
            while ((firstLine = in.readLine()) != null) {
                logger.info("read " + firstLine);
                String res = cp.process(firstLine);
                logger.info("processed command, write: " + res);
                out.write(res + "\r\n");
                out.flush();
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
