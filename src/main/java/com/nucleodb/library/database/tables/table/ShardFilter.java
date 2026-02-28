package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.ShardAssignment;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Update;

/**
 * Shard-aware filter for DataTable operations.
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

    public boolean create(Create c) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(c.getKey());
    }

    public <T extends DataEntry> boolean delete(Delete d, T existing) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(d.getKey());
    }

    public <T extends DataEntry> boolean update(Update u, T existing) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(u.getKey());
    }

    public <T extends DataEntry> boolean accept(String key) {
        if (shardAssignment == null) return true;
        return shardAssignment.owns(key);
    }

    public ShardAssignment getShardAssignment() {
        return shardAssignment;
    }
}
