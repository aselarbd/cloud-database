package de.tum.i13.client;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.client.communication.impl.SocketStreamCloserFactory;
import de.tum.i13.shared.Constants;
import de.tum.i13.shared.commandparsers.StringArrayCommandParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class KvClient {

    private final static Logger LOGGER = Logger.getLogger(KvClient.class.getName());
    private final static String PROMPT = "EchoClient> ";
    private final static String LOG_LVL_NAMES = "ALL | CONFIG | FINE | FINEST | INFO | OFF | SEVERE | WARNING";
    private SocketCommunicator communicator;
    private BufferedReader inReader;
    private Map<String, Action> actions;

    public KvClient() {
        this.communicator = new SocketCommunicatorImpl();
        this.communicator.init(new SocketStreamCloserFactory(), Constants.TELNET_ENCODING);
        this.inReader = new BufferedReader(new InputStreamReader(System.in));
        this.actions = new HashMap<>();
        this.actions.put("connect", new Action<String[]>(
                new StringArrayCommandParser( 2, false), this::connect));
        this.actions.put("disconnect", new Action<String[]>(
                new StringArrayCommandParser(0, false), this::disconnect));
        this.actions.put("send", new Action<String[]>(
                new StringArrayCommandParser(2, true), this::send));
        this.actions.put("logLevel", new Action<String[]>(
                new StringArrayCommandParser(1, false), this::logLevel));
    }

    /**
     * Prints out a prompt and reads the input from command line.
     *
     * @return A string array containing the command line splitted by spaces
     * @throws IOException if reading fails
     */
    private String readPromptLine() throws IOException {
        System.out.print(PROMPT);
        String line = inReader.readLine();
        // treat null values or empty strings (such as after Ctrl+D) as quit command
        if (line == null || line.length() == 0) {
            line = "quit";
        }
        // only split by one space instead of multiple ones, to keep multiple spaces like
        // in "send foo    bar"
        return line.trim();
    }

    /**
     * Convenience function to print a line. Also removes duplicate newlines.
     *
     * @param line The String to print out
     */
    private void write(String line) {
        if (line.endsWith("\r\n")) {
            line = line.substring(0, line.length() - 2);
        }
        System.out.println(PROMPT + line);
    }

    /**
     * Writes a line by calling write() and also logs the content with level WARNING.
     *
     * @param line String to output
     */
    private void writeAndWarn(String line) {
        write(line);
        LOGGER.warning(line);
    }

    /**
     * Writes a line by calling write() and adds an INFO log entry.
     *
     * @param line String to output
     */
    private void writeAndLog(String line) {
        write(line);
        LOGGER.info(line);
    }

    /**
     * Logic for the help command
     */
    private void help() {
        write("The EchoClient allows to send messages to a server and prints out\n"
                + "the server's replies.\n\n"
                + "Available commands:\n"
                + "\thelp\tPrints out this help message\n"
                + "\tquit\tExits the client\n"
                + "\tconnect <host> <port>\tConnects to the given server\n"
                + "\tsend <message>\tSends the given message to the server (requires connection)\n"
                + "\tdisconnect\tDisconnects from the server\n"
                + "\tlogLevel <level>\tSets the log level to one of\n"
                + "\t\t" + LOG_LVL_NAMES
        );
    }

    /**
     * Logic for connect <name> <port>
     *
     * @param args The input arguments splitted by spaces
     */
    private void connect(String[] args) {
        String hostName = args[0];
        int port;
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            write("Not a valid port: " + args[1]);
            return;
        }
        LOGGER.fine("Connecting to " + hostName + ":" + args[1]);
        String resp = null;
        try {
            resp = communicator.connect(hostName, port);
            write(resp);
        } catch (SocketCommunicatorException e) {
            writeAndWarn("Unable to connect: " + e.getMessage());
            LOGGER.warning("Failed connection was to " + hostName + ":" + args[1]);
        }
    }

    /**
     * Logic for disconnect
     *
     * @param args The input arguments splitted by spaces
     */
    private void disconnect(String[] args) {
        try {
            communicator.disconnect();
        } catch (SocketCommunicatorException e) {
            writeAndWarn("Unable to disconnect: " + e.getMessage());
        }
    }

    /**
     * Logic for send <message>
     *
     * @param args The input arguments splitted by spaces
     */
    private void send(String[] args) {
        // re-construct message string
        String toSend = String.join(" ", args);
        toSend = toSend.trim();

        try {
            String result = communicator.send(toSend);
            write(result);
        } catch(SocketCommunicatorException e) {
            writeAndWarn("Could not send message: " + e.getMessage());
            LOGGER.warning("Failed message was: " + toSend);
        }
    }

    /**
     * Logic for logLevel <level>
     *
     * @param args The input arguments splitted by spaces
     */
    private void logLevel(String[] args) {
        // Level.parse also accepts numbers. Ensure only specified strings are accepted.
        final String level = args[0];
        if (level.matches("[A-Z]+") && LOG_LVL_NAMES.contains(level)) {
            try {
                Level newLevel = Level.parse(level);
                // use unnamed logger such that settings are applied for all loggers
                Logger.getLogger("").setLevel(newLevel);
                // log is obviously only visible for appropriate levels. Print out user feedback as well.
                writeAndLog("New log Level is " + level);
            } catch (IllegalArgumentException e) {
                // use WARNING here as this should never happen due to the checks above
                LOGGER.log(Level.WARNING, "Error while parsing log level", e);
                write("Could not parse log level. Use one of");
                write(LOG_LVL_NAMES);
            }
        } else {
            write("Please choose one of the following levels:");
            write(LOG_LVL_NAMES);
            return;
        }
    }

    /**
     * Runs the main loop of the command line client.
     *
     * @throws Exception if any uncaught error occurs during execution
     */
    public void run() throws Exception {
        boolean exit = false;
        while (!exit) {
            String inputMsg = readPromptLine();

            // handle quit case separately as it exits the loop
            if (inputMsg.equals("quit")) {
                write("EchoClient is going to exit now. Goodbye!");
                // ensure disconnect. By specification, calling this when not connected does nothing
                communicator.disconnect();
                exit = true;
            } else {
                // Look up what to do
                Action<?> action = actions.get(inputMsg);
                if (action != null) {
                    // parse the line and run the appropriate command
                    action.run(String.join(" ", inputMsg));
                } else {
                    // default action is help
                    if (!inputMsg.equals("help")) {
                        write("Unknown command: " + inputMsg);
                        write("");
                    }
                    help();
                }
            }
        }
    }

    public static void main(String[] args) {
        setupLogging(Path.of("client.log"), "ALL");

        LOGGER.info("Creating a new Socket");
        KvClient client = new KvClient();

        try {
            client.run();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception occurred in main()", e);
        }
    }
}
