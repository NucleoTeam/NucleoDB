package com.nucleodb.library.mqs.kafka.ratis;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class NucleoDBStateMachineTest {

    private NucleoDBStateMachine stateMachine;

    @BeforeEach
    public void setup() {
        stateMachine = new NucleoDBStateMachine();
    }

    @Test
    public void committedEntriesStartsEmpty() {
        assertTrue(stateMachine.getCommittedEntries().isEmpty());
    }

    @Test
    public void applyTransactionWithKeyValueSeparator() throws Exception {
        String key = "myKey";
        String value = "myValue";
        String combined = key + '\0' + value;

        TransactionContext trx = mockTransaction(combined, 1, 1);
        CompletableFuture<Message> result = stateMachine.applyTransaction(trx);

        assertNotNull(result);
        assertEquals("OK", result.get().getContent().toString(StandardCharsets.UTF_8));

        assertEquals(1, stateMachine.getCommittedEntries().size());
        NucleoDBStateMachine.CommittedEntry entry = stateMachine.getCommittedEntries().poll();
        assertNotNull(entry);
        assertEquals("myKey", entry.getKey());
        assertEquals("myValue", entry.getValue());
        assertEquals(1, entry.getIndex());
    }

    @Test
    public void applyTransactionWithoutSeparator() throws Exception {
        String message = "messageWithNoSeparator";

        TransactionContext trx = mockTransaction(message, 1, 2);
        stateMachine.applyTransaction(trx);

        assertEquals(1, stateMachine.getCommittedEntries().size());
        NucleoDBStateMachine.CommittedEntry entry = stateMachine.getCommittedEntries().poll();
        assertNotNull(entry);
        assertEquals("", entry.getKey());
        assertEquals("messageWithNoSeparator", entry.getValue());
        assertEquals(2, entry.getIndex());
    }

    @Test
    public void applyTransactionWithEmptyKey() throws Exception {
        String combined = '\0' + "valueOnly";

        TransactionContext trx = mockTransaction(combined, 1, 3);
        stateMachine.applyTransaction(trx);

        NucleoDBStateMachine.CommittedEntry entry = stateMachine.getCommittedEntries().poll();
        assertNotNull(entry);
        assertEquals("", entry.getKey());
        assertEquals("valueOnly", entry.getValue());
    }

    @Test
    public void applyTransactionWithEmptyValue() throws Exception {
        String combined = "keyOnly" + '\0';

        TransactionContext trx = mockTransaction(combined, 1, 4);
        stateMachine.applyTransaction(trx);

        NucleoDBStateMachine.CommittedEntry entry = stateMachine.getCommittedEntries().poll();
        assertNotNull(entry);
        assertEquals("keyOnly", entry.getKey());
        assertEquals("", entry.getValue());
    }

    @Test
    public void applyMultipleTransactions() throws Exception {
        for (int i = 0; i < 5; i++) {
            TransactionContext trx = mockTransaction("key" + i + '\0' + "val" + i, 1, i + 1);
            stateMachine.applyTransaction(trx);
        }

        assertEquals(5, stateMachine.getCommittedEntries().size());

        // Entries should come out in order
        for (int i = 0; i < 5; i++) {
            NucleoDBStateMachine.CommittedEntry entry = stateMachine.getCommittedEntries().poll();
            assertNotNull(entry);
            assertEquals("key" + i, entry.getKey());
            assertEquals("val" + i, entry.getValue());
            assertEquals(i + 1, entry.getIndex());
        }
    }

    @Test
    public void queryReturnsOK() throws Exception {
        CompletableFuture<Message> result = stateMachine.query(Message.valueOf("anything"));
        assertNotNull(result);
        assertEquals("OK", result.get().getContent().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void applyTransactionWithMultipleSeparators() throws Exception {
        // Only the first null byte should be treated as separator
        String combined = "key" + '\0' + "value" + '\0' + "extra";

        TransactionContext trx = mockTransaction(combined, 1, 10);
        stateMachine.applyTransaction(trx);

        NucleoDBStateMachine.CommittedEntry entry = stateMachine.getCommittedEntries().poll();
        assertNotNull(entry);
        assertEquals("key", entry.getKey());
        assertEquals("value" + '\0' + "extra", entry.getValue());
    }

    private TransactionContext mockTransaction(String data, long term, long index) {
        ByteString byteString = ByteString.copyFrom(data, StandardCharsets.UTF_8);

        RaftProtos.StateMachineLogEntryProto smLogEntry =
            RaftProtos.StateMachineLogEntryProto.newBuilder()
                .setLogData(byteString)
                .build();

        RaftProtos.LogEntryProto logEntry = RaftProtos.LogEntryProto.newBuilder()
            .setTerm(term)
            .setIndex(index)
            .setStateMachineLogEntry(smLogEntry)
            .build();

        TransactionContext trx = mock(TransactionContext.class);
        when(trx.getLogEntry()).thenReturn(logEntry);
        return trx;
    }
}
