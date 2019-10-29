package de.tum.i13.client;

import de.tum.i13.communication.SocketCommunicator;
import de.tum.i13.communication.SocketCommunicatorException;
import de.tum.i13.communication.impl.SocketCommunicatorImpl;
import de.tum.i13.communication.impl.SocketStreamCloserFactory;
import de.tum.i13.shared.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class KvClient {

    private final static Logger LOGGER = Logger.getLogger(KvClient.class.getName());
    private final static String PROMPT = "EchoClient> ";
    private final static String LOG_LVL_NAMES = "ALL | CONFIG | FINE | FINEST | INFO | OFF | SEVERE | WARNING";
    private SocketCommunicator communicator;
    private BufferedReader inReader;

    public KvClient() {
        this.communicator = new SocketCommunicatorImpl();
        this.communicator.init(new SocketStreamCloserFactory(), Constants.TELNET_ENCODING);
        this.inReader = new BufferedReader(new InputStreamReader(System.in));
    }

    /**
     * Prints out a prompt and reads the input from command line.
     *
     * @return A string array containing the command line splitted by spaces
     * @throws IOException if reading fails
     */
    private String[] readPromptLine() throws IOException {
        System.out.print(PROMPT);
        String line = inReader.readLine();
        // treat null values or empty strings (such as after Ctrl+D) as quit command
        if (line == null || line.length() == 0) {
            line = "quit";
        }
        // only split by one space instead of multiple ones, to keep multiple spaces like
        // in "send foo    bar"
        return line.trim().split("\\s");
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
     * Helper to validate command input.
     *
     * @param cmdLine Input splitted by {@link #readPromptLine()}
     * @param count Expected item count, at least or equal
     * @param expectEqual if true, <code>cmdLine</code> has to contain exactly <code>count</code> arguments.
     *                    Otherwise,<code>cmdLine</code> needs more than <code>count</code> items.
     * @return True if the command has invalid arguments, false otherwise.
     */
    private boolean hasWrongArgs(String[] cmdLine, int count, boolean expectEqual) {
        String errorMsg = "";
        if (cmdLine == null || cmdLine.length == 0) {
            write("Command can't be empty.");
            help();
            return true;
        } else {
            errorMsg = cmdLine[0] + " requires ";
        }

        boolean invalidCmd = false;
        if (expectEqual && cmdLine.length != count) {
            errorMsg += "exactly ";
            invalidCmd = true;
        } else if (!expectEqual && cmdLine.length < count) {
            errorMsg += "at least ";
            invalidCmd = true;
        }

        if (invalidCmd) {
            errorMsg += Integer.toString(count - 1) + " arguments.";
            write(errorMsg);
            help();
        }
        return invalidCmd;
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
     * @param cmdLine The input string splitted by spaces
     */
    private void connect(String[] cmdLine) {
        if (hasWrongArgs(cmdLine, 3, true)) {
            return;
        }

        String hostName = cmdLine[1];
        int port;
        try {
            port = Integer.parseInt(cmdLine[2]);
        } catch (NumberFormatException e) {
            write("Not a valid port: " + cmdLine[2]);
            return;
        }
        LOGGER.fine("Connecting to " + hostName + ":" + cmdLine[2]);
        String resp = null;
        try {
            resp = communicator.connect(hostName, port);
            write(resp);
        } catch (SocketCommunicatorException e) {
            writeAndWarn("Unable to connect: " + e.getMessage());
            LOGGER.warning("Failed connection was to " + hostName + ":" + cmdLine[2]);
        }
    }

    /**
     * Logic for disconnect
     *
     * @param cmdLine The input string splitted by spaces
     */
    private void disconnect(String[] cmdLine) {
        if (hasWrongArgs(cmdLine, 1, true)) {
            return;
        }

        try {
            communicator.disconnect();
        } catch (SocketCommunicatorException e) {
            writeAndWarn("Unable to disconnect: " + e.getMessage());
        }
    }

    /**
     * Logic for send <message>
     *
     * @param cmdLine The input string splitted by spaces
     */
    private void send(String[] cmdLine) {
        if (hasWrongArgs(cmdLine, 2, false)) {
            return;
        }

        // re-construct message string
        String toSend = "";
        for (int i=1; i < cmdLine.length; ++i) {
            toSend += cmdLine[i] + " ";
        }
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
     * @param cmdLine The input string splitted by spaces
     */
    private void logLevel(String[] cmdLine) {
        if (hasWrongArgs(cmdLine, 2, true)) {
            return;
        }

        // Level.parse also accepts numbers. Ensure only specified strings are accepted.
        final String level = cmdLine[1];
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
            String[] inputMsg = readPromptLine();
            String cmd = inputMsg[0];

            switch (cmd) {
                case "quit":
                    if (inputMsg.length == 1) {
                        write("EchoClient is going to exit now. Goodbye!");
                        // ensure disconnect. By specification, calling this when not connected does nothing
                        communicator.disconnect();
                        exit = true;
                    } else {
                        help();
                    }
                    break;
                case "connect":
                    connect(inputMsg);
                    break;
                case "disconnect":
                    disconnect(inputMsg);
                    break;
                case "send":
                    send(inputMsg);
                    break;
                case "logLevel":
                    logLevel(inputMsg);
                    break;
                default:
                    write("Unknown command: " + cmd);
                    write("");
                    help();
                    break;
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
