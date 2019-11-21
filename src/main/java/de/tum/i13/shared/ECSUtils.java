package de.tum.i13.shared;

import java.net.InetSocketAddress;
import java.util.*;

public class ECSUtils {

    /**
     * add new ip:port to a ECS server
     * @param address : newly added server IP:PORT
     * @param metadataTable : meta data table reference that need to update
     */
    public void rebalancedServerRangesAdd(InetSocketAddress address, ConsistentHashMap metadataTable) {

        Map <InetSocketAddress, String> ipAndMessageList = ipAndMessageList(address,metadataTable,"add");
        //TODO: Broadcast to all servers

    }

    /**
     * removed ip:port of server
     * @param address : removed server IP:PORT
     * @param metadataTable : meta data table reference that need to update
     */
    public void rebalancedServerRangesRemove(InetSocketAddress address, ConsistentHashMap metadataTable) {

        Map <InetSocketAddress, String> ipAndMessageList = ipAndMessageList(address,metadataTable,"remove");

        //TODO: Broadcast to all servers
    }

    private Map<InetSocketAddress, String> ipAndMessageList(InetSocketAddress ip,ConsistentHashMap metadataTable, String type){
        String message = null;
        String newServerMessage = null;
        Map<InetSocketAddress, String> ipAndMessageList = new HashMap<>();

        if (type.equals("add")){
            metadataTable.put(ip);

            ECSMessage messageToAllServers = new ECSMessage(ECSMessage.MsgType.BROADCAST_NEW);
            messageToAllServers.addIpPort(0, ip);
            message =  messageToAllServers.getFullMessage();

            ECSMessage messageToNewServer =  new ECSMessage(ECSMessage.MsgType.KEYRANGE);
            messageToNewServer.addKeyrange(0,metadataTable);
            newServerMessage = messageToNewServer.getFullMessage();
        }
        if (type.equals("remove")){
            metadataTable.remove(ip);

            ECSMessage messageToAllServers = new ECSMessage(ECSMessage.MsgType.BROADCAST_REM);
            messageToAllServers.addIpPort(0, ip);
            message =  messageToAllServers.getFullMessage();
        }

        ArrayList<InetSocketAddress> broadcastIPList = getIPAddressList(metadataTable);

        for (InetSocketAddress ipAddress: broadcastIPList ){
            if (type.equals("add") && ipAddress.equals(ip)){
                ipAndMessageList.put(ipAddress, newServerMessage);
            }else {
                ipAndMessageList.put(ipAddress, message);
            }
        }
        return ipAndMessageList;
    }

    public ArrayList<InetSocketAddress> getIPAddressList(ConsistentHashMap metadataTable)
            throws IllegalArgumentException {

        String keyRange = metadataTable.getKeyrangeString();
        ArrayList<InetSocketAddress> ipAddressList = new ArrayList<>();

        if (!keyRange.contains(";")) {
            throw new IllegalArgumentException("Bad format: No semicolon found");
        }

        String[] elements = keyRange.split(";");
        InetSocketAddressTypeConverter converter = new InetSocketAddressTypeConverter();

        for (String element : elements) {
            String[] elemParts = element.split(",");

            if (elemParts.length != 3) {
                throw new IllegalArgumentException(
                        "Bad format: expecting start_hash,end_hash,ip:port but got "
                                + element);
            }

            try {
                // only parse IP and add it, the hashes are checked later
                InetSocketAddress addr = converter.convert(elemParts[2]);
                ipAddressList.add(addr);
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not parse ip:port", e);
            }
        }

        return ipAddressList;
    }

}
