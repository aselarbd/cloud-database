package de.tum.i13.client;

import de.tum.i13.shared.LogSetup;
import de.tum.i13.shared.LogeLevelChange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;

import java.util.logging.Logger;


public class Milestone1Main {

    public static Logger logger = Logger.getLogger(Milestone1Main.class.getName());

    public static void main(String[] args) throws IOException {

        Milestone1Main mm = new Milestone1Main();
        mm.start();
    }

    private void start() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        ActiveConnection activeConnection = null;
        for(;;) {
            System.out.print("EchoClient> ");
            String line = reader.readLine();
            logger.finest(line);
            if(!line.isEmpty()) {
                String[] command = line.split(" ");
                logger.fine(String.format("command: %s", command));
                switch (command[0]) {
                    case "connect":
                        activeConnection = buildConnection(command);
                        break;
                    case "send":
                        sendmessage(activeConnection, command, line);
                        break;
                    case "disconnect":
                        closeConnection(activeConnection);
                        break;
                    case "loglevel":
                        changeLogLevel(command);
                        break;
                    case "help":
                        printHelp();
                        break;
                    case "quit":
                        printEchoLine("Application exit!");
                        return;
                    default:
                        retUnknownCommand();
                }
            }
        }
    }

    private void changeLogLevel(String[] command) {
        if(command.length != 2) {
            retUnknownCommand();
            return;
        }
        try {
            Level level = Level.parse(command[1]);
            LogeLevelChange logeLevelChange = LogSetup.changeLoglevel(level);
            printEchoLine(String.format("loglevel changed from: %s to: %s", logeLevelChange.getPreviousLevel(), logeLevelChange.getNewLevel()));

        } catch (IllegalArgumentException ex) {
            printEchoLine("Unknown loglevel");
        }

    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("connect <address> <port> - Tries to establish a TCP- connection to the echo server based on the given server address and the port number of the echo service.");
        System.out.println("disconnect - Tries to disconnect from the connected server.");
        System.out.println("send <message> - Sends a text message to the echo server according to the communication protocol.");
        System.out.println(String.format("logLevel <level> - Sets the logger to the specified log level (%s | DEBUG | INFO | WARN | ERROR | FATAL | OFF)", Level.ALL.getName()));
        System.out.println("help - Display this help");
        System.out.println("quit - Tears down the active connection to the server and exits the program execution.");
    }

    private void retUnknownCommand() {
        printEchoLine("Unknown command");
    }

    private void printEchoLine(String msg) {
        System.out.println("EchoClient> " + msg);
    }

    private static void closeConnection(ActiveConnection activeConnection) {
        if(activeConnection != null) {
            try {
                activeConnection.close();
            } catch (Exception e) {
                //e.printStackTrace();
                //TODO: handle gracefully
                activeConnection = null;
            }
        }
    }

    private void sendmessage(ActiveConnection activeConnection, String[] command, String line) {
        if(activeConnection == null) {
            printEchoLine("Error! Not connected!");
            return;
        }
        int firstSpace = line.indexOf(" ");
        if(firstSpace == -1 || firstSpace + 1 >= line.length()) {
            printEchoLine("Error! Nothing to send!");
            return;
        }

        String cmd = line.substring(firstSpace + 1);
        activeConnection.write(cmd);

        try {
            printEchoLine(activeConnection.readline());
        } catch (IOException e) {
            printEchoLine("Error! Not connected!");
        }
    }

    private ActiveConnection buildConnection(String[] command) {
        if(command.length == 3){
            try {
                var kvcb = new EchoConnectionBuilder(command[1], Integer.parseInt(command[2]));
                logger.info("begin connecting");
                ActiveConnection ac = kvcb.connect();
                logger.info("connected");
                String confirmation = ac.readline();
                printEchoLine(confirmation);
                return ac;
            } catch (Exception e) {
                logger.severe(e.toString());
                //Todo: separate between could not connect, unknown host and invalid port
                printEchoLine("Could not connect to server");
            }
        }
        return null;
    }
}
