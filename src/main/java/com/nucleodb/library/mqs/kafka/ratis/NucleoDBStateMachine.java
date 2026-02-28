package com.nucleodb.library.mqs.kafka.ratis;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Ratis state machine that receives committed Raft log entries and
 * feeds them into NucleoDB's internal message queue for processing.
 * Kafka is used as the communication fabric between Raft peers.
 */
public class NucleoDBStateMachine extends BaseStateMachine {
    private static final Logger logger = Logger.getLogger(NucleoDBStateMachine.class.getName());

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final ConcurrentLinkedQueue<CommittedEntry> committedEntries = new ConcurrentLinkedQueue<>();

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        logger.info("NucleoDBStateMachine initialized for group " + groupId);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        final ByteString data = entry.getStateMachineLogEntry().getLogData();
        final String message = data.toString(StandardCharsets.UTF_8);

        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());

        // Parse key and value from the committed message
        int separatorIndex = message.indexOf('\0');
        String key;
        String value;
        if (separatorIndex >= 0) {
            key = message.substring(0, separatorIndex);
            value = message.substring(separatorIndex + 1);
        } else {
            key = "";
            value = message;
        }

        committedEntries.add(new CommittedEntry(key, value, entry.getIndex()));

        return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    public ConcurrentLinkedQueue<CommittedEntry> getCommittedEntries() {
        return committedEntries;
    }

    /**
     * A committed Raft log entry ready for downstream processing.
     */
    public static class CommittedEntry {
        private final String key;
        private final String value;
        private final long index;

        public CommittedEntry(String key, String value, long index) {
            this.key = key;
            this.value = value;
            this.index = index;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public long getIndex() {
            return index;
        }
    }
}
