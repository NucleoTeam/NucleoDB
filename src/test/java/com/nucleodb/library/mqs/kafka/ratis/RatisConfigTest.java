package com.nucleodb.library.mqs.kafka.ratis;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RatisConfigTest {

    @Test
    public void defaultConstructor() {
        RatisConfig config = new RatisConfig();
        assertNull(config.getSelfId());
        assertEquals(9860, config.getPort());
        assertEquals(new File("/tmp/nucleodb-ratis"), config.getStorageDir());
        assertTrue(config.getPeers().isEmpty());
    }

    @Test
    public void parameterizedConstructor() {
        File storageDir = new File("/data/ratis");
        RatisConfig config = new RatisConfig("node1", 9870, storageDir);

        assertEquals("node1", config.getSelfId());
        assertEquals(9870, config.getPort());
        assertEquals(storageDir, config.getStorageDir());
        assertTrue(config.getPeers().isEmpty());
    }

    @Test
    public void addPeerReturnsSelfForChaining() {
        RatisConfig config = new RatisConfig("n0", 9860, new File("/tmp"));
        RatisConfig returned = config.addPeer("n0", "127.0.0.1:9860");
        assertSame(config, returned);
    }

    @Test
    public void addMultiplePeers() {
        RatisConfig config = new RatisConfig("n0", 9860, new File("/tmp"))
            .addPeer("n0", "127.0.0.1:9860")
            .addPeer("n1", "127.0.0.1:9861")
            .addPeer("n2", "127.0.0.1:9862");

        assertEquals(3, config.getPeers().size());
        assertEquals("n0", config.getPeers().get(0).getId());
        assertEquals("127.0.0.1:9860", config.getPeers().get(0).getAddress());
        assertEquals("n1", config.getPeers().get(1).getId());
        assertEquals("127.0.0.1:9861", config.getPeers().get(1).getAddress());
        assertEquals("n2", config.getPeers().get(2).getId());
        assertEquals("127.0.0.1:9862", config.getPeers().get(2).getAddress());
    }

    @Test
    public void setters() {
        RatisConfig config = new RatisConfig();
        config.setSelfId("nodeX");
        config.setPort(1234);
        config.setStorageDir(new File("/custom/path"));

        assertEquals("nodeX", config.getSelfId());
        assertEquals(1234, config.getPort());
        assertEquals(new File("/custom/path"), config.getStorageDir());
    }

    @Test
    public void setPeersReplacesList() {
        RatisConfig config = new RatisConfig();
        config.addPeer("n0", "127.0.0.1:9860");
        assertEquals(1, config.getPeers().size());

        List<RatisConfig.RatisPeerConfig> newPeers = new ArrayList<>();
        newPeers.add(new RatisConfig.RatisPeerConfig("a", "10.0.0.1:9860"));
        newPeers.add(new RatisConfig.RatisPeerConfig("b", "10.0.0.2:9860"));
        config.setPeers(newPeers);

        assertEquals(2, config.getPeers().size());
        assertEquals("a", config.getPeers().get(0).getId());
    }

    @Test
    public void peerConfigDefaultConstructor() {
        RatisConfig.RatisPeerConfig peer = new RatisConfig.RatisPeerConfig();
        assertNull(peer.getId());
        assertNull(peer.getAddress());
    }

    @Test
    public void peerConfigSetters() {
        RatisConfig.RatisPeerConfig peer = new RatisConfig.RatisPeerConfig();
        peer.setId("p1");
        peer.setAddress("192.168.1.1:9860");
        assertEquals("p1", peer.getId());
        assertEquals("192.168.1.1:9860", peer.getAddress());
    }
}
