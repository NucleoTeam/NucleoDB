package com.nucleodb.library;

/**
 * Strategy interface for determining shard ownership of keys.
 * Implementations decide whether a given key belongs to this node's shard.
 */
public interface ShardAssignment {
    /**
     * Returns true if this shard owns the given key.
     */
    boolean owns(String key);

    /**
     * Returns the shard index that should own the given key.
     */
    int getShardForKey(String key);
}
