package de.tum.i13.client;


import de.tum.i13.client.subscription.SubscriptionService;
import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.KVTP2ClientFactory;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.shared.*;
import de.tum.i13.shared.parsers.KVItemParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Consumer;

/**
 * Library to interact with a key-value server.
 */
public class KVLib {
    private final static Log logger = new Log(KVLib.class);

    private ConsistentHashMap keyRanges;
    private ConsistentHashMap keyRangesReplica;
    private KVTP2ClientFactory clientFactory;
    private Map<InetSocketAddress, KVTP2Client> clientMap = new HashMap<>();

    private final Map<String, Integer> requestFailureCounts = new HashMap<>();
    private static final int MAX_RETRIES = 10;

    public KVLib() {
        this(KVTP2Client::new);
    }

    public KVLib(KVTP2ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        this.requestFailureCounts.put("put", 0);
        this.requestFailureCounts.put("get", 0);
        this.requestFailureCounts.put("delete", 0);
    }

    /**
     * Helper to send a KVTP2 message in telnet format, get the response and return it as string.
     *
     * IMPORTANT: This only works for sending messages *without* arguments! If you need arguments, construct
     * the messages by yourself.
     *
     * @param client Client which should send the message
     * @param cmd The command to send
     * @return
     * @throws IOException
     */
    private String sendV1Msg(KVTP2Client client, String cmd) throws IOException {
        Message res = client.send(new Message(cmd, Message.Version.V1));
        // ignore malformed message errors
        String val = res.get("original");
        if (val == null) {
            val = res.toString();
        }
        return val;
    }

    /**
     * scan returns a list of partially key matched item
     *
     * @param item partial key item
     * @return KV Item list
     */
    public String scan (KVItem item){
        if (keyRanges == null || keyRanges.size() <= 0) {
            return "no server started";
        }

        Map <String ,KVItem> resultMap = new HashMap<>();
        StringBuilder errorMessage = new StringBuilder();
        StringBuilder successMessage = new StringBuilder();
        String serverMessage = "";

        connectToAllKVServers();
        if (!clientMap.isEmpty()) {
            Iterator<Map.Entry<InetSocketAddress, KVTP2Client>> it = clientMap.entrySet().iterator();
            while (it.hasNext()){
                Map.Entry<InetSocketAddress, KVTP2Client> anyCom = it.next();
                try {
                    Message scanReq = new Message("scan", Message.Version.V2);
                    scanReq.put("partialKey", item.getKey());
                    Message scanResp = anyCom.getValue().send(scanReq);
                    if (scanResp == null || scanResp.getCommand().equals("_error")) {
                        continue;
                    }
                    if (scanResp.getCommand().equals("server_stopped")) {
                        // just skip server
                    } else if (scanResp.getCommand().equals("scan_error")){
                        errorMessage.append(scanResp.toString());
                    }
                    else {
                        serverMessage = scanResp.getCommand();

                        int kvCount = Integer.parseInt(scanResp.get("count"));
                        for (int i=1; i<=kvCount; i++){
                            String k = scanResp.get("K"+i);
                            String v = scanResp.get("V"+i);
                            if (!resultMap.containsKey(k)){
                                KVItem decodedItem = new KVItem(k);
                                decodedItem.setValueFrom64(v);
                                resultMap.put(k, decodedItem);
                            }
                        }
                    }
                } catch (IOException e) {
                    it.remove();
                }
            }
        }

        if (resultMap.size() > 0){
            successMessage.append(serverMessage).append("  <").append(item.getKey()).append("> ");
            for (KVItem k: resultMap.values()){
                successMessage.append(k.getKey()).append(":").append(k.getValue()).append(", ");
            }
            return successMessage.substring(0,successMessage.length() -2);
        }else {
            return errorMessage.toString();
        }
    }

    private void connectToAllKVServers () {
        Set<InetSocketAddress> serversList = keyRanges.getAllServerList();

        for (InetSocketAddress server : serversList){
            try {
                connect(server.getHostString(),server.getPort());
            } catch (IOException e) {
                logger.warning("Connection issue in " + server.getHostString() + ":" + server.getPort(), e);
            }
        }
    }

    /**
     * get key range for new servers
     *
     * @return key range for each server that got by server or server stop message
     * if server not responding
     */
    public String keyRange() {
        getKeyRanges();
        if (keyRanges == null){
            logger.warning("Metadata table on the kV Client is empty");
            return "Server Doesn't have key range values";
        }
        logger.info("Generate Key range");
        return keyRanges.getKeyrangeString();
    }

    /**
     * get key range for new servers
     *
     * @return key range for each server that got by server or server stop message
     * if server not responding
     */
    public String keyRangeRead() {
        getKeyRanges();
        if (keyRangesReplica == null){
            logger.warning("Metadata table on the kV Client is empty");
            return "Server Doesn't have key range values";
        }
        logger.info("Generate Key range");
        return keyRangesReplica.getKeyrangeReadString();
    }

    /**
     * Connect to the given server.
     *
     * @param address The server IP address as String
     * @param port The port number as int
     *
     * @return the message returned by the server or
     * a useful error message if no connection could be established
     *
     * @throws java.io.IOException if the connection fails.
     */
    public String connect(String address, int port) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(address, port);
        if (clientMap.get(addr) != null) {
            return "already connected";
        }
        KVTP2Client client = clientFactory.get(address, port);
        client.connect();
        String res = sendV1Msg(client, "connected");
        clientMap.put(new InetSocketAddress(address, port), client);
        getKeyRanges();
        return res;
    }

    public void changeServerLogLevel(String level){
        if (keyRanges == null || keyRanges.size() <= 0) {
            return;
        }
        connectToAllKVServers();

        if (!clientMap.isEmpty()) {
            Iterator<Map.Entry<InetSocketAddress,KVTP2Client>> it = clientMap.entrySet().iterator();
            while (it.hasNext()){
                Map.Entry<InetSocketAddress,KVTP2Client> anyCom = it.next();
                try {
                    Message logLvlMsg = new Message("serverLogLevel", Message.Version.V1);
                    logLvlMsg.put("level", level);
                    Message res = anyCom.getValue().send(logLvlMsg);
                    if (res == null) {
                        continue;
                    }
                    String logLevelResponse = res.toString();
                    if (logLevelResponse.equals("server_stopped")) {
                        continue;
                    }
                    InetSocketAddress serverIp = anyCom.getKey();
                    logger.info(serverIp.getHostString() + ":" + serverIp.getPort() + " " + logLevelResponse);

                } catch (IOException e) {
                    it.remove();
                }
            }
        }
    }

    private void dropClient(InetSocketAddress address) {
        KVTP2Client comm = clientMap.get(address);
        if (comm.isConnected()) {
            try {
                comm.close();
            } catch (IOException e) {
                // no problem, we want to drop the client anyway
            }
        }
        clientMap.remove(address);
    }

    public SubscriptionService getSubscriptionService(Consumer<KVItem> updateHandler, Consumer<String> outputHandler) {
        return new SubscriptionService(this::subscriptionServiceKeyrangeUpdate, updateHandler, outputHandler);
    }

    private ConsistentHashMap subscriptionServiceKeyrangeUpdate() {
        getKeyRanges();
        return keyRanges;
    }

    private String getKeyRangeStr(String cmd, KVTP2Client client)
            throws IOException {
        String keyRangeResponse = sendV1Msg(client, cmd);
        if (keyRangeResponse == null || keyRangeResponse.isEmpty()) {
            logger.warning("Got empty response for keyrange");
            return null;
        }
        if (keyRangeResponse.equals("server_stopped")) {
            return null;
        }
        String[] responseSplitted = keyRangeResponse.split("\\s+");
        if (responseSplitted.length < 2) {
            logger.warning("Invalid keyrange response: " + keyRangeResponse);
            return null;
        }
        return responseSplitted[1];
    }

    private synchronized void getKeyRanges() {
        if (!clientMap.isEmpty()) {
            Iterator<Map.Entry<InetSocketAddress, KVTP2Client>> it = clientMap.entrySet().iterator();
            while (it.hasNext()){
                    Map.Entry<InetSocketAddress, KVTP2Client> anyCom = it.next();
                try {
                    String keyRangeString = getKeyRangeStr("keyrange", anyCom.getValue());
                    if (keyRangeString == null) {
                        continue;
                    }
                    keyRanges = ConsistentHashMap.fromKeyrangeString(keyRangeString);
                    keyRangeString = getKeyRangeStr("keyrange_read", anyCom.getValue());
                    if (keyRangeString == null) {
                        continue;
                    }
                    keyRangesReplica = ConsistentHashMap.fromKeyrangeReadString(keyRangeString);
                    return;
                } catch (IOException e) {
                    it.remove();
                } catch (IllegalArgumentException e) {
                    logger.warning("Got invalid keyrange", e);
                }
            }
        }
        // everything is empty. Reset keyranges and communicator map
        keyRanges = null;
        keyRangesReplica = null;
        clientMap = new HashMap<>();
    }

    private void incrementFailures(String op) {
        requestFailureCounts.put(op, requestFailureCounts.get(op) + 1);
    }

    /**
     * Common logic for all operations
     *
     * @param op operation name (get, put, delete)
     * @param item Item to be processed
     * @return Server reply encoded as {@link KVResult}
     */
    private KVResult kvOperation(String op, KVItem item) {
        if (item == null || !item.isValid() || (op.equals("put") && item.getValue() == null)) {
            return new KVResult("Invalid key-value item");
        }
        if (keyRanges == null || keyRanges.size() <= 0) {
            return new KVResult("no server started");
        }
        if (requestFailureCounts.get(op) > MAX_RETRIES) {
            requestFailureCounts.put(op, 0);
            logger.info("Exceeded maximum retries. Aborting.");
            return new KVResult("Server error");
        }

        final InetSocketAddress targetServer;

        if (op.equals("get")){
            List <InetSocketAddress> ipList = keyRangesReplica.getAllSuccessors(item.getKey());
            targetServer = ipList.get(new Random().nextInt(ipList.size()));
        } else {
            targetServer = keyRanges.getSuccessor(item.getKey());
        }

        if (!clientMap.containsKey(targetServer)) {
            String address = targetServer.getHostString();
            int port = targetServer.getPort();
            try {
                connect(address, port);
            } catch (IOException e) {
                incrementFailures(op);
                getKeyRanges();
                return kvOperation(op, item);
            }
        }

        KVTP2Client client = clientMap.get(targetServer);

        if (!client.isConnected()) {
            return new KVResult("not connected");
        }
        try {
            // in case of put, we need to encode the value first
            KVItem sendItem;
            if (op.equals("put")) {
                sendItem = new KVItem(item.getKey(), item.getValueAs64());
                // check encoded length again
                if (!sendItem.isValid()) {
                    return new KVResult("Value too long");
                }
            } else {
                sendItem = item;
            }
            Message sendMsg = new Message(op, Message.Version.V1);
            sendMsg.put("key", sendItem.getKey());
            if (sendItem.getValue() != null) {
                sendMsg.put("value", sendItem.getValue());
            }
            Message resMsg = client.send(sendMsg);
            if (resMsg == null) {
                return new KVResult("Empty response");
            }
            KVResult res = new KVResult(resMsg);
            if (op.equals("get") && res.getItem() == null) {
                requestFailureCounts.put(op, 0);
                return new KVResult("Empty response");
            } else if (res.getMessage().equals("get_error")) {
                requestFailureCounts.put(op, 0);
                return res;
            } else if (res.getMessage().equals("server_not_responsible") ||
                    res.getMessage().equals("server_stopped")) {
                incrementFailures(op);
                int timeout = getBackOffTime(requestFailureCounts.get(op));
                Thread.sleep(timeout);
                getKeyRanges();
                return kvOperation(op, item);
            } else if (!op.equals("get") && res.getMessage().equals("server_write_lock")) {
                requestFailureCounts.put(op, 0);
                return new KVResult("server locked, please try later");
            }
            requestFailureCounts.put(op, 0);
            // decode the value if present
            return res.decoded();
        } catch (IOException e) {
            // server might just got removed, retry once before aborting
            if (requestFailureCounts.get(op) < MAX_RETRIES - 1) {
                requestFailureCounts.put(op, MAX_RETRIES - 1);
                // delete the communicator and update key ranges
                dropClient(targetServer);
                getKeyRanges();
                return kvOperation(op, item);
            } else {
                logger.warning("Error in put()", e);
                requestFailureCounts.put(op, 0);
                return new KVResult("Server error");
            }
        } catch (InterruptedException e) {
            logger.warning("Error in put() -> timeout()", e);
            requestFailureCounts.put(op, 0);
            return new KVResult("Server error");
        }
    }


    /**
     * Put a key-value pair to the server.
     *
     * @param item the Key-Value item to put
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult put(KVItem item) {
        return kvOperation("put", item);
    }

    /**
     * Get a key-value pair
     * @param item Key to query, given as {@link KVItem}. The value is ignored.
     * @return Server reply encoded as {@link KVResult}, which also contains the respective {@link KVItem} if
     *  found.
     */
    public KVResult get(KVItem item) {
        return kvOperation("get", item);
    }

    /**
     * Delete a key-value pair.
     *
     * @param item Key to query, given as {@link KVItem}. The value is ignored.
     * @return Server reply encoded as {@link KVResult}
     */
    public KVResult delete(KVItem item) {
        return kvOperation("delete", item);
    }

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     */
    public String disconnect() {
        StringBuilder res = new StringBuilder();
        for (Map.Entry<InetSocketAddress, KVTP2Client> s : clientMap.entrySet()) {
            try {
                s.getValue().close();
                res.append("Disconnected from ").append(InetSocketAddressTypeConverter.addrString(s.getKey())).append("\n");
            } catch (IOException e) {
                logger.warning("Could not close connection", e);
                res.append("Unable to disconnect from ").append(InetSocketAddressTypeConverter.addrString(s.getKey())).append(" - ").append(e.getMessage()).append("\n");
            }
        }
        // use an empty map again for the next connection
        clientMap = new HashMap<>();
        return res.toString();
    }

    /**
     * gives backoff time for client requests
     * @param attempt: no of current attempts
     * @return backoff time as int
     */
    private int getBackOffTime(int attempt) {
        final int maxBackoffTime = 1000;
        final int baseBackOffTime = 10;

        int backOffTime = Math.min(maxBackoffTime, baseBackOffTime * 2 ^ attempt);
        return new Random().nextInt(backOffTime +1);
    }

}
