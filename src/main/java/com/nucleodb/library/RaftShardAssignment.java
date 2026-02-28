package com.nucleodb.library;

/**
 * Raft-consensus-backed shard assignment.
 * <p>
 * This is the extension point for dynamic shard assignment via Raft consensus.
 * When fully implemented, this class will:
 * <ul>
 *   <li>Participate in Raft leader election among cluster nodes</li>
 *   <li>Maintain a replicated shard assignment map via the Raft state machine</li>
 *   <li>Automatically reassign shards when nodes join or leave the cluster</li>
 * </ul>
 * <p>
 * Currently delegates to {@link HashShardAssignment} with a static shard ID.
 * To integrate a Raft library (e.g. Apache Ratis), extend this class and
 * override {@link #owns(String)} and {@link #getShardForKey(String)} to query
 * the Raft-managed assignment state.
 */
public class RaftShardAssignment implements ShardAssignment {

    private final HashShardAssignment delegate;

    public RaftShardAssignment(int shardId, int totalShards) {
        this.delegate = new HashShardAssignment(shardId, totalShards);
    }

    @Override
    public boolean owns(String key) {
        return delegate.owns(key);
    }

    @Override
    public int getShardForKey(String key) {
        return delegate.getShardForKey(key);
    }
}
