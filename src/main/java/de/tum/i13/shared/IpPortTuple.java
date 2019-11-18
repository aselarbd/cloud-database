package de.tum.i13.shared;

/**
 * Represents a pair of IP address and port
 */
public class IpPortTuple {
    private String ip;
    private int port;

    public IpPortTuple(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public IpPortTuple(String colonSeparated) {
        String[] res = colonSeparated.split(":");
        this.ip = res[0];
        this.port = Integer.valueOf(res[1]);
    }

    public String getIp() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }

    public String getColonSeparated() {
        return this.ip + ":" + this.port;
    }
}
