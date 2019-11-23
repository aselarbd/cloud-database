package de.tum.i13.shared;

import de.tum.i13.kvtp.Server;

import java.net.InetSocketAddress;
import java.util.*;

public class ECSUtils {

    private Server sender;

    public ECSUtils(Server sender) {
        this.sender = sender;
    }

    /**
     * add new ip:port to a ECS server
     * @param address : newly added server IP:PORT
     * @param metadataTable : meta data table reference that need to update
     */
    public void rebalancedServerRangesAdd(InetSocketAddress address, ConsistentHashMap metadataTable) {

        Map <InetSocketAddress, String> ipAndMessageList = ipAndMessageList(address,metadataTable,"add");
        broadcast(ipAndMessageList);
    }

    /**
     * removed ip:port of server
     * @param address : removed server IP:PORT
     * @param metadataTable : meta data table reference that need to update
     */
    public void rebalancedServerRangesRemove(InetSocketAddress address, ConsistentHashMap metadataTable) {

        Map <InetSocketAddress, String> ipAndMessageList = ipAndMessageList(address,metadataTable,"remove");
        broadcast(ipAndMessageList);
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

        ArrayList<InetSocketAddress> broadcastIPList = metadataTable.getIPAddressList();

        for (InetSocketAddress ipAddress: broadcastIPList ){
            if (type.equals("add") && ipAddress.equals(ip)){
                ipAndMessageList.put(ipAddress, newServerMessage);
            }else {
                ipAndMessageList.put(ipAddress, message);
            }
        }
        return ipAndMessageList;
    }

    void broadcast(Map<InetSocketAddress,String > messageList){
        for (InetSocketAddress ip : messageList.keySet()){
            sender.sendTo(ip, messageList.get(ip));
        }
    }
}
