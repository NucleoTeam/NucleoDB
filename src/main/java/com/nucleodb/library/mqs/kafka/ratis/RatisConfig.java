package com.nucleodb.library.mqs.kafka.ratis;

import org.apache.ratis.protocol.RaftPeer;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for a Ratis cluster node.
 * Defines the peers, ports, and storage for the Raft consensus group.
 */
public class RatisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<RatisPeerConfig> peers = new ArrayList<>();
    private String selfId;
    private File storageDir = new File("/tmp/nucleodb-ratis");
    private int port = 9860;

    public RatisConfig() {
    }

    public RatisConfig(String selfId, int port, File storageDir) {
        this.selfId = selfId;
        this.port = port;
        this.storageDir = storageDir;
    }

    public RatisConfig addPeer(String id, String address) {
        peers.add(new RatisPeerConfig(id, address));
        return this;
    }

    public List<RatisPeerConfig> getPeers() {
        return peers;
    }

    public void setPeers(List<RatisPeerConfig> peers) {
        this.peers = peers;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setSelfId(String selfId) {
        this.selfId = selfId;
    }

    public File getStorageDir() {
        return storageDir;
    }

    public void setStorageDir(File storageDir) {
        this.storageDir = storageDir;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Configuration for a single Raft peer.
     */
    public static class RatisPeerConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private String id;
        private String address;

        public RatisPeerConfig() {
        }

        public RatisPeerConfig(String id, String address) {
            this.id = id;
            this.address = address;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }
}
