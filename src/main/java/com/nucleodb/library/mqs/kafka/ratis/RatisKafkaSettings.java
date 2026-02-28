package com.nucleodb.library.mqs.kafka.ratis;

import com.nucleodb.library.mqs.kafka.KafkaSettings;

import java.io.File;
import java.util.Map;
import java.util.UUID;

/**
 * Settings for the Ratis+Kafka MQS implementation.
 * Extends KafkaSettings with Ratis-specific configuration for the Raft cluster.
 */
public class RatisKafkaSettings extends KafkaSettings {

    private RatisConfig ratisConfig;

    public RatisKafkaSettings(Map<String, Object> objs) {
        super(objs);

        Object ratisConfigObj = objs.get("ratisConfig");
        if (ratisConfigObj instanceof RatisConfig) {
            this.ratisConfig = (RatisConfig) ratisConfigObj;
        } else {
            // Default single-node Ratis config
            String nodeId = System.getenv().getOrDefault("RATIS_NODE_ID", "n0");
            String nodeAddress = System.getenv().getOrDefault("RATIS_NODE_ADDRESS", "127.0.0.1:9860");
            int port = Integer.parseInt(System.getenv().getOrDefault("RATIS_PORT", "9860"));
            String storageDir = System.getenv().getOrDefault("RATIS_STORAGE_DIR", "/tmp/nucleodb-ratis");

            this.ratisConfig = new RatisConfig(nodeId, port, new File(storageDir));
            this.ratisConfig.addPeer(nodeId, nodeAddress);

            // Add additional peers from environment if configured
            String peersEnv = System.getenv("RATIS_PEERS");
            if (peersEnv != null && !peersEnv.isEmpty()) {
                for (String peerStr : peersEnv.split(",")) {
                    String[] parts = peerStr.split("=");
                    if (parts.length == 2) {
                        this.ratisConfig.addPeer(parts[0].trim(), parts[1].trim());
                    }
                }
            }
        }
    }

    public RatisConfig getRatisConfig() {
        return ratisConfig;
    }

    public void setRatisConfig(RatisConfig ratisConfig) {
        this.ratisConfig = ratisConfig;
    }
}
