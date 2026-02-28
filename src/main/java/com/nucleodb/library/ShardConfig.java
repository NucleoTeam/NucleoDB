package com.nucleodb.library;

import java.io.Serializable;

/**
 * Configuration for shard-based data partitioning.
 */
public class ShardConfig implements Serializable {
    private static final long serialVersionUID = 1;

    private int shardId;
    private int totalShards;
    private ShardAssignment shardAssignment;

    public ShardConfig() {
        this.shardId = 0;
        this.totalShards = 1;
        this.shardAssignment = new HashShardAssignment(0, 1);
    }

    public ShardConfig(int shardId, int totalShards) {
        this.shardId = shardId;
        this.totalShards = totalShards;
        this.shardAssignment = new HashShardAssignment(shardId, totalShards);
    }

    public ShardConfig(ShardAssignment shardAssignment) {
        this.shardAssignment = shardAssignment;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public int getTotalShards() {
        return totalShards;
    }

    public void setTotalShards(int totalShards) {
        this.totalShards = totalShards;
    }

    public ShardAssignment getShardAssignment() {
        return shardAssignment;
    }

    public void setShardAssignment(ShardAssignment shardAssignment) {
        this.shardAssignment = shardAssignment;
    }
}
