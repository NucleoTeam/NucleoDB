package com.nucleodb.library.mqs.kafka.ratis;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CommittedEntryTest {

    @Test
    public void constructorAndGetters() {
        NucleoDBStateMachine.CommittedEntry entry =
            new NucleoDBStateMachine.CommittedEntry("testKey", "testValue", 42);

        assertEquals("testKey", entry.getKey());
        assertEquals("testValue", entry.getValue());
        assertEquals(42, entry.getIndex());
    }

    @Test
    public void emptyKeyAndValue() {
        NucleoDBStateMachine.CommittedEntry entry =
            new NucleoDBStateMachine.CommittedEntry("", "", 0);

        assertEquals("", entry.getKey());
        assertEquals("", entry.getValue());
        assertEquals(0, entry.getIndex());
    }

    @Test
    public void largeIndex() {
        NucleoDBStateMachine.CommittedEntry entry =
            new NucleoDBStateMachine.CommittedEntry("k", "v", Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, entry.getIndex());
    }

    @Test
    public void valueWithSpecialCharacters() {
        String value = "{\"name\":\"test\",\"data\":[1,2,3]}";
        NucleoDBStateMachine.CommittedEntry entry =
            new NucleoDBStateMachine.CommittedEntry("json-key", value, 100);

        assertEquals("json-key", entry.getKey());
        assertEquals(value, entry.getValue());
    }
}
