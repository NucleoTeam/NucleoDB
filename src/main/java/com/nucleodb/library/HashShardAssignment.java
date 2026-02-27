package com.nucleodb.library;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hash ring implementation of ShardAssignment.
 * <p>
 * Uses virtual nodes placed on a circular hash space (0 to Integer.MAX_VALUE)
 * to ensure even distribution of keys across shards. Each shard is represented
 * by multiple virtual nodes on the ring to minimize key redistribution when
 * shards are added or removed.
 */
public class HashShardAssignment implements ShardAssignment, Serializable {
    private static final long serialVersionUID = 1;
    private static final int DEFAULT_VIRTUAL_NODES = 150;

    private final int shardId;
    private final int totalShards;
    private final int virtualNodes;
    private final transient SortedMap<Integer, Integer> ring;

    public HashShardAssignment(int shardId, int totalShards) {
        this(shardId, totalShards, DEFAULT_VIRTUAL_NODES);
    }

    public HashShardAssignment(int shardId, int totalShards, int virtualNodes) {
        if (shardId < 0 || shardId >= totalShards) {
            throw new IllegalArgumentException(
                String.format("shardId %d must be in range [0, %d)", shardId, totalShards)
            );
        }
        if (totalShards < 1) {
            throw new IllegalArgumentException("totalShards must be >= 1");
        }
        if (virtualNodes < 1) {
            throw new IllegalArgumentException("virtualNodes must be >= 1");
        }
        this.shardId = shardId;
        this.totalShards = totalShards;
        this.virtualNodes = virtualNodes;
        this.ring = buildRing();
    }

    private SortedMap<Integer, Integer> buildRing() {
        SortedMap<Integer, Integer> ring = new TreeMap<>();
        for (int shard = 0; shard < totalShards; shard++) {
            for (int v = 0; v < virtualNodes; v++) {
                String virtualNodeKey = "shard-" + shard + "-vn-" + v;
                int hash = hash(virtualNodeKey);
                ring.put(hash, shard);
            }
        }
        return ring;
    }

    @Override
    public boolean owns(String key) {
        return getShardForKey(key) == shardId;
    }

    @Override
    public int getShardForKey(String key) {
        if (ring.isEmpty()) {
            return 0;
        }
        int hash = hash(key);
        // Walk clockwise on the ring to find the first node >= hash
        SortedMap<Integer, Integer> tailMap = ring.tailMap(hash);
        int ringPosition = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        return ring.get(ringPosition);
    }

    /**
     * Hash a string to a position on the ring using MD5 for uniform distribution.
     */
    static int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // Use first 4 bytes to construct a positive int
            return ((digest[0] & 0xFF) << 24)
                 | ((digest[1] & 0xFF) << 16)
                 | ((digest[2] & 0xFF) << 8)
                 | (digest[3] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            // MD5 is always available in standard JDK
            throw new RuntimeException(e);
        }
    }

    public int getShardId() {
        return shardId;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getVirtualNodes() {
        return virtualNodes;
    }
}
