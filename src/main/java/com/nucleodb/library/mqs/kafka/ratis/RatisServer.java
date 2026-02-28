package com.nucleodb.library.mqs.kafka.ratis;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of a Ratis Raft server for NucleoDB consensus.
 * Uses gRPC for inter-node communication and Kafka as the data fabric.
 */
public class RatisServer implements Closeable {
    private static final Logger logger = Logger.getLogger(RatisServer.class.getName());

    // Fixed group ID for NucleoDB consensus group
    private static final UUID GROUP_UUID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

    private final org.apache.ratis.server.RaftServer server;
    private final NucleoDBStateMachine stateMachine;
    private final RaftGroup raftGroup;

    public RatisServer(RatisConfig config) throws IOException {
        this.stateMachine = new NucleoDBStateMachine();

        List<RaftPeer> peers = config.getPeers().stream()
            .map(p -> RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(p.getId()))
                .setAddress(p.getAddress())
                .build())
            .collect(Collectors.toList());

        RaftGroupId groupId = RaftGroupId.valueOf(GROUP_UUID);
        this.raftGroup = RaftGroup.valueOf(groupId, peers);

        RaftProperties properties = new RaftProperties();

        // Configure storage
        File storageDir = new File(config.getStorageDir(), config.getSelfId());
        RaftServerConfigKeys.setStorageDir(properties, List.of(storageDir));

        // Configure gRPC port
        GrpcConfigKeys.Server.setPort(properties, config.getPort());

        // Build the Raft server
        this.server = org.apache.ratis.server.RaftServer.newBuilder()
            .setGroup(raftGroup)
            .setServerId(RaftPeerId.valueOf(config.getSelfId()))
            .setProperties(properties)
            .setStateMachine(stateMachine)
            .build();

        logger.info("RatisServer created with id=" + config.getSelfId() +
            " port=" + config.getPort() +
            " peers=" + peers.stream().map(p -> p.getId().toString()).collect(Collectors.joining(",")));
    }

    public void start() throws IOException {
        server.start();
        logger.info("RatisServer started");
    }

    @Override
    public void close() throws IOException {
        server.close();
        logger.info("RatisServer stopped");
    }

    public org.apache.ratis.server.RaftServer getServer() {
        return server;
    }

    public NucleoDBStateMachine getStateMachine() {
        return stateMachine;
    }

    public RaftGroup getRaftGroup() {
        return raftGroup;
    }

    public boolean isLeader() {
        try {
            return server.getDivision(raftGroup.getGroupId()).getInfo().isLeader();
        } catch (IOException e) {
            return false;
        }
    }
}
