package com.nucleodb.library.mqs.kafka.ratis;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

public class RatisKafkaSettingsTest {

    @Test
    public void constructWithExplicitRatisConfig() {
        RatisConfig ratisConfig = new RatisConfig("n0", 9870, new File("/data/ratis"))
            .addPeer("n0", "10.0.0.1:9870")
            .addPeer("n1", "10.0.0.2:9871");

        Map<String, Object> settingsMap = new TreeMap<>();
        settingsMap.put("servers", "kafka1:9092,kafka2:9092");
        settingsMap.put("groupName", "test-group");
        settingsMap.put("partitions", 3);
        settingsMap.put("replicas", 1);
        settingsMap.put("topic", "test-topic");
        settingsMap.put("ratisConfig", ratisConfig);

        RatisKafkaSettings settings = new RatisKafkaSettings(settingsMap);

        assertSame(ratisConfig, settings.getRatisConfig());
        assertEquals("kafka1:9092,kafka2:9092", settings.getServers());
        assertEquals("test-group", settings.getGroupName());
        assertEquals(3, settings.getPartitions());
        assertEquals(1, settings.getReplicas());
        assertEquals("test-topic", settings.getTable());
    }

    @Test
    public void constructWithDefaultRatisConfig() {
        Map<String, Object> settingsMap = new TreeMap<>();
        settingsMap.put("topic", "test-topic");

        RatisKafkaSettings settings = new RatisKafkaSettings(settingsMap);

        assertNotNull(settings.getRatisConfig());
        // Default config creates a single-node cluster
        assertFalse(settings.getRatisConfig().getPeers().isEmpty());
    }

    @Test
    public void setRatisConfig() {
        Map<String, Object> settingsMap = new TreeMap<>();
        settingsMap.put("topic", "test-topic");
        RatisKafkaSettings settings = new RatisKafkaSettings(settingsMap);

        RatisConfig newConfig = new RatisConfig("x0", 5000, new File("/new/path"));
        settings.setRatisConfig(newConfig);

        assertSame(newConfig, settings.getRatisConfig());
    }

    @Test
    public void inheritsKafkaSettings() {
        Map<String, Object> settingsMap = new TreeMap<>();
        settingsMap.put("servers", "broker1:9092");
        settingsMap.put("groupName", "my-group");
        settingsMap.put("partitions", 12);
        settingsMap.put("replicas", 2);
        settingsMap.put("offsetReset", "latest");
        settingsMap.put("topic", "my-topic");

        RatisKafkaSettings settings = new RatisKafkaSettings(settingsMap);

        assertEquals("broker1:9092", settings.getServers());
        assertEquals("my-group", settings.getGroupName());
        assertEquals(12, settings.getPartitions());
        assertEquals(2, settings.getReplicas());
        assertEquals("latest", settings.getOffsetReset());
        assertEquals("my-topic", settings.getTable());
    }

    @Test
    public void nonRatisConfigObjectFallsBackToDefaults() {
        Map<String, Object> settingsMap = new TreeMap<>();
        settingsMap.put("topic", "test-topic");
        settingsMap.put("ratisConfig", "not-a-ratis-config-object");

        RatisKafkaSettings settings = new RatisKafkaSettings(settingsMap);

        // Should create a default config rather than crash
        assertNotNull(settings.getRatisConfig());
        assertNotNull(settings.getRatisConfig().getSelfId());
    }
}
