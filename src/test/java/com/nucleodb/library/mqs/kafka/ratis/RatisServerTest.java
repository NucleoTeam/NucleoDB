package com.nucleodb.library.mqs.kafka.ratis;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class RatisServerTest {

    @TempDir
    File tempDir;

    private RatisServer ratisServer;
    private RatisConfig config;

    @BeforeEach
    public void setup() throws IOException {
        config = new RatisConfig("n0", 19860, tempDir)
            .addPeer("n0", "127.0.0.1:19860");

        ratisServer = new RatisServer(config);
        ratisServer.start();
    }

    @AfterEach
    public void teardown() throws IOException {
        if (ratisServer != null) {
            ratisServer.close();
        }
    }

    @Test
    public void serverStartsAndStops() {
        assertNotNull(ratisServer.getServer());
        assertNotNull(ratisServer.getStateMachine());
        assertNotNull(ratisServer.getRaftGroup());
    }

    @Test
    public void stateMachineIsAccessible() {
        NucleoDBStateMachine sm = ratisServer.getStateMachine();
        assertNotNull(sm);
        assertTrue(sm.getCommittedEntries().isEmpty());
    }

    @Test
    public void raftGroupHasOnePeer() {
        assertEquals(1, ratisServer.getRaftGroup().getPeers().size());
    }

    @Test
    public void singleNodeBecomesLeader() throws InterruptedException {
        // Single-node cluster should elect itself as leader quickly
        boolean isLeader = false;
        for (int i = 0; i < 50; i++) {
            if (ratisServer.isLeader()) {
                isLeader = true;
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(isLeader, "Single node should become leader");
    }

    @Test
    public void submitAndCommitEntry() throws Exception {
        // Wait for leader election
        for (int i = 0; i < 50; i++) {
            if (ratisServer.isLeader()) break;
            Thread.sleep(100);
        }
        assertTrue(ratisServer.isLeader(), "Must be leader to submit");

        // Create a Raft client
        RaftProperties raftProperties = new RaftProperties();
        try (RaftClient client = RaftClient.newBuilder()
                .setRaftGroup(ratisServer.getRaftGroup())
                .setClientRpc(new GrpcFactory(new org.apache.ratis.conf.Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties))
                .setProperties(raftProperties)
                .build()) {

            String key = "author_123";
            String value = "CREATEsomeJsonData";
            String combined = key + '\0' + value;

            RaftClientReply reply = client.io().send(Message.valueOf(combined));
            assertTrue(reply.isSuccess());

            // Verify the state machine received the committed entry
            NucleoDBStateMachine.CommittedEntry entry = ratisServer.getStateMachine().getCommittedEntries().poll();
            assertNotNull(entry, "State machine should have a committed entry");
            assertEquals("author_123", entry.getKey());
            assertEquals("CREATEsomeJsonData", entry.getValue());
        }
    }

    @Test
    public void submitMultipleEntries() throws Exception {
        for (int i = 0; i < 50; i++) {
            if (ratisServer.isLeader()) break;
            Thread.sleep(100);
        }

        RaftProperties raftProperties = new RaftProperties();
        try (RaftClient client = RaftClient.newBuilder()
                .setRaftGroup(ratisServer.getRaftGroup())
                .setClientRpc(new GrpcFactory(new org.apache.ratis.conf.Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties))
                .setProperties(raftProperties)
                .build()) {

            for (int i = 0; i < 10; i++) {
                String combined = "key" + i + '\0' + "value" + i;
                RaftClientReply reply = client.io().send(Message.valueOf(combined));
                assertTrue(reply.isSuccess(), "Entry " + i + " should commit successfully");
            }

            assertEquals(10, ratisServer.getStateMachine().getCommittedEntries().size());

            // Verify ordering
            for (int i = 0; i < 10; i++) {
                NucleoDBStateMachine.CommittedEntry entry =
                    ratisServer.getStateMachine().getCommittedEntries().poll();
                assertNotNull(entry);
                assertEquals("key" + i, entry.getKey());
                assertEquals("value" + i, entry.getValue());
            }
        }
    }

    @Test
    public void queryReturnsOK() throws Exception {
        for (int i = 0; i < 50; i++) {
            if (ratisServer.isLeader()) break;
            Thread.sleep(100);
        }

        RaftProperties raftProperties = new RaftProperties();
        try (RaftClient client = RaftClient.newBuilder()
                .setRaftGroup(ratisServer.getRaftGroup())
                .setClientRpc(new GrpcFactory(new org.apache.ratis.conf.Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties))
                .setProperties(raftProperties)
                .build()) {

            RaftClientReply reply = client.io().sendReadOnly(Message.valueOf("test"));
            assertTrue(reply.isSuccess());
            assertEquals("OK", reply.getMessage().getContent().toString(StandardCharsets.UTF_8));
        }
    }
}
