package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.ShardAssignment;
import com.nucleodb.library.database.modifications.ConnectionCreate;
import com.nucleodb.library.database.modifications.ConnectionDelete;
import com.nucleodb.library.database.modifications.ConnectionUpdate;

/**
 * Shard-aware filter for Connection operations.
 * Replaces NodeFilter by delegating ownership checks to a {@link ShardAssignment}.
 * <p>
 * Each method can be overridden for custom filtering logic beyond shard ownership.
 */
public class ShardFilter {
    private final ShardAssignment shardAssignment;

    /**
     * Creates a pass-through filter that accepts everything (single-shard mode).
     */
    public ShardFilter() {
        this.shardAssignment = null;
    }

    public ShardFilter(ShardAssignment shardAssignment) {
        this.shardAssignment = shardAssignment;
    }

    public boolean create(ConnectionCreate c) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(c.getUuid());
    }

    public <C extends Connection> boolean delete(ConnectionDelete d, C existing) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(d.getUuid());
    }

    public <C extends Connection> boolean update(ConnectionUpdate u, C existing) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(u.getUuid());
    }

    public <C extends Connection> boolean accept(String key) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(key);
    }

    public ShardAssignment getShardAssignment() {
        return shardAssignment;
    }
}
