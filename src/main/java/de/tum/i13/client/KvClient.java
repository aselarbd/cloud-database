package de.tum.i13.client;

import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.subscription.SubscriptionService;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import de.tum.i13.shared.Log;
import de.tum.i13.shared.LogLevelChange;
import de.tum.i13.shared.LogSetup;
import de.tum.i13.shared.TaskRunner;
import de.tum.i13.shared.parsers.KVItemParser;
import de.tum.i13.shared.parsers.StringArrayParser;

import java.io.*;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class KvClient {

    private final static Log logger = new Log(KvClient.class);
    private final static String PROMPT = "EchoClient> ";
    private final static String LOG_LVL_NAMES = "ALL | CONFIG | FINE | FINEST | INFO | OFF | SEVERE | WARNING";
    private final KVLib kvLib;
    private final SubscriptionService subscriptionService;
    private final BufferedReader inReader;
    private final Map<String, Action> actions;

    public KvClient() {
        this(new InputStreamReader(System.in));
    }

    public KvClient(Reader inReader) {
        this.kvLib = new KVLib();
        this.subscriptionService = this.kvLib.getSubscriptionService(
                kvItem -> {
                    String delSuffix = "";
                    if (!kvItem.hasValue()) {
                        delSuffix = " got deleted";
                    }

                    // do not use write() to get nicer output (no double prompt)
                    System.out.println("Subscription update: " + kvItem.toString() + delSuffix);
                    // add new prompt
                    System.out.print(PROMPT);
                },
                this::write);
        this.inReader = new BufferedReader(inReader);
        this.actions = new HashMap<>();
        this.actions.put("connect", new Action<>(
                new StringArrayParser(2, false), this::connect));
        this.actions.put("disconnect", new Action<>(
                new StringArrayParser(0, false), this::disconnect));
        this.actions.put("put", new Action<>(
                new KVItemParser(true), this::put
        ));
        this.actions.put("get", new Action<>(
                new KVItemParser(false), this::get
        ));
        this.actions.put("delete", new Action<>(
                new KVItemParser(false), this::delete
        ));
        this.actions.put("scan", new Action<>(
                new KVItemParser(false), this::scan
        ));
        this.actions.put("subscribe", new Action<>(
                new KVItemParser(false),
                kvItem -> write(this.subscriptionService.subscribe(kvItem.getKey()))
        ));
        this.actions.put("unsubscribe", new Action<>(
                new KVItemParser(false),
                kvItem -> write(this.subscriptionService.unsubscribe(kvItem.getKey()))
        ));
        this.actions.put("logLevel", new Action<>(
                new StringArrayParser(1, false), this::logLevel));
        this.actions.put("keyrange", new Action<>(
                new StringArrayParser(0, false), this::keyRange));
        this.actions.put("keyrange_read", new Action<>(
                new StringArrayParser(0, false), this::keyRangeRead));
        this.actions.put("serverLogLevel", new Action<>(
                new StringArrayParser(1, false), this::serverLogLevel));
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
        // treat null values (such as after Ctrl+D) as quit command
        if (line == null ) {
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
        logger.warning(line);
    }

    /**
     * Writes a line by calling write() and adds an INFO log entry.
     *
     * @param line String to output
     */
    private void writeAndLog(String line) {
        write(line);
        logger.info(line);
    }


    /**
     * KeyRange function will get the key ranges from server and output the write-keyrange
     *
     * @param args : arguments from commandline
     */
    private void keyRange(String[] args) {
        String res = kvLib.keyRange();
        write(res);
    }

    /**
     * KeyRangeRead function will get the key range from server and output the read-keyrange
     *
     * @param args : arguments from commandline
     */
    private void keyRangeRead(String[] args) {
        String res = kvLib.keyRangeRead();
        write(res);
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
                + "\tkeyrange\tGets keyrange data and outputs the keyrange without replica\n"
                + "\tkeyrange_read\tGets keyrange data and outputs the keyrange including replica\n"
                + "\tput <key> <value>\tPuts the given key-value pair to the server (requires connection)\n"
                + "\tget <key>\tGets the key-value pair with the given key from the server (requires connection)\n"
                + "\tscan <key>\tGet all partial matching key value pairs from the server (requires connection)\n"
                + "\tdelete <key>\tDeletes the key-value pair with the given key from the server (requires connection)\n"
                + "\tsubscribe <key>\tGives notifications about the key's value updates (requires connection)\n"
                + "\tunsubscribe <key>\tStop sending notifications about key's value updates (requires connection)\n"
                + "\tdisconnect\tDisconnects from the server\n"
                + "\tlogLevel <level>\tSets the log level to one of\n"
                + "\t\t" + LOG_LVL_NAMES+"\n"
                + "\tserverLogLevel <level>\tSets the server log level to one of\n"
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
        logger.fine("Connecting to " + hostName + ":" + args[1]);
        String resp;
        try {
            resp = kvLib.connect(hostName, port);
            write(resp);
        } catch (IOException e) {
            writeAndWarn("Unable to connect: " + e.getMessage());
            logger.warning("Failed connection was to " + hostName + ":" + args[1]);
        }
    }

    /**
     * Logic for disconnect
     *
     * @param args The input arguments splitted by spaces
     */
    private void disconnect(String[] args) {
        write(kvLib.disconnect());
    }

    /**
     * Logic for the put command.
     *
     * @param item Parsed item
     */
    private void put(KVItem item) {
        KVResult res = kvLib.put(item);
        write(res.toString());
    }

    /**
     * Logic for the get command.
     *
     * @param item Parsed item
     */
    private void get(KVItem item) {
        KVResult res = kvLib.get(item);
        write(res.toString());
    }

    /**
     * Logic for the delete command.
     *
     * @param item Parsed item
     */
    private void delete(KVItem item) {
        KVResult res = kvLib.delete(item);
        write(res.toString());
    }

    /**
     * Logic for scan command
     *
     * @param item Passed item
     */
    private void scan (KVItem item){
        String scanResult = kvLib.scan(item);
        write(scanResult);

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
                LogLevelChange change = LogSetup.changeLoglevel(newLevel);
                // log is obviously only visible for appropriate levels. Print out user feedback as well.
                writeAndLog("Changed log Level from " + change.getPreviousLevel().toString() + " to "
                                + change.getNewLevel().toString());
            } catch (IllegalArgumentException e) {
                // use WARNING here as this should never happen due to the checks above
                logger.warning("Error while parsing log level", e);
                write("Could not parse log level. Use one of");
                write(LOG_LVL_NAMES);
            }
        } else {
            write("Please choose one of the following levels:");
            write(LOG_LVL_NAMES);
        }
    }

    private void serverLogLevel (String[] args) {
        final String level = args[0];
        if(level.matches("[A-Z]+") && LOG_LVL_NAMES.contains(level)){
             kvLib.changeServerLogLevel(level);
        }else {
            write("Please choose one of the following levels:");
            write(LOG_LVL_NAMES);
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
                kvLib.disconnect();
                subscriptionService.quit();
                exit = true;
            } else if (inputMsg.equals("")) {
                // if user press enter key just give prompt
            } else {
                // Look up what to do
                Action<?> action = actions.get(inputMsg.split(" ")[0]);
                boolean actionResult = false;
                if (action != null) {
                    // parse the line and run the appropriate command
                    actionResult = action.run(inputMsg);
                }

                if(!actionResult) {
                    // default action is help
                    if (!inputMsg.equals("help")) {
                        write("Unknown or invalid command: " + inputMsg);
                        write("");
                    }
                    help();
                }
            }
        }
    }
    public static void main(String[] args) {
        setupLogging(Path.of("client.log"), "ALL");

        logger.info("Creating a new Socket");
        KvClient client = new KvClient();

        try {
            client.run();
        } catch (Exception e) {
            logger.severe("Exception occurred in main()", e);
        }
    }
}
